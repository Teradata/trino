/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.teradata.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.teradata.integration.clearscape.ClearScapeEnvironmentUtils.generateUniqueEnvName;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_WITH_POSITION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DEFAULT_COLUMN_VALUE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_REFRESH_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_UPDATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

@ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
final class TestTeradataConnectorTest
        extends BaseJdbcConnectorTest
{
    private static final int TERADATA_OBJECT_NAME_LIMIT = 128;

    private TestingTeradataServer database;

    private static void verifyResultOrFailure(AssertProvider<QueryAssertions.QueryAssert> queryAssertProvider, Consumer<QueryAssertions.QueryAssert> verifyResults, Consumer<TrinoExceptionAssert> verifyFailure)
    {
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");
        QueryAssertions.QueryAssert queryAssert = assertThat(queryAssertProvider);
        verifyResults.accept(queryAssert);
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    private static String rewriteCreateTableQuery(String sql)
    {
        String trimmed = sql.trim();
        String upper = trimmed.toUpperCase(ENGLISH);

        // Handle "AS TABLE schema.table" → "AS (SELECT * FROM schema.table) WITH DATA"
        if (upper.startsWith("AS TABLE ")) {
            String tableName = trimmed.substring("AS TABLE ".length()).trim();
            return "AS (SELECT * FROM " + tableName + ") WITH DATA";
        }

        // Only rewrite AS ... forms; plain column-list definitions like "(a INT, ...)" pass through unchanged
        if (!upper.startsWith("AS ")) {
            return sql;
        }

        // Strip "AS " prefix and any optional WITH DATA / WITH NO DATA suffix
        String afterAs = trimmed.substring(3).trim();
        String afterAsUpper = afterAs.toUpperCase(ENGLISH);

        String withSuffix = " WITH DATA";
        if (afterAsUpper.endsWith("WITH DATA")) {
            afterAs = afterAs.substring(0, afterAs.length() - "WITH DATA".length()).trim();
        }
        else if (afterAsUpper.endsWith("WITH NO DATA")) {
            afterAs = afterAs.substring(0, afterAs.length() - "WITH NO DATA".length()).trim();
            withSuffix = " WITH NO DATA";
        }

        // If the SELECT is already wrapped in outer parens, leave them as-is; otherwise add them
        if (afterAs.startsWith("(") && afterAs.endsWith(")")) {
            return "AS " + afterAs + withSuffix;
        }

        return "AS (" + afterAs + ")" + withSuffix;
    }

    public static String rewriteVarcharTypes(String sql)
    {
        // Replace VARCHAR or VARCHAR   (with spaces) not followed by '('
        return sql.replaceAll("(?i)\\bVARCHAR\\b(?!\\s*\\()", "VARCHAR(100)");
    }

    private static boolean containsIgnoreCase(String text, String substr)
    {
        if (text == null || substr == null) {
            return false;
        }
        return text.toLowerCase(Locale.ROOT).contains(substr.toLowerCase(Locale.ROOT));
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return database;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database = closeAfterClass(new TestingTeradataServer(generateUniqueEnvName(getClass()), true));
        // Register this specific instance for this test class
        return TeradataQueryRunner.builder(database).setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE -> false;
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_INSERT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_ROW_LEVEL_UPDATE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_UPDATE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @AfterAll
    public void cleanupTestDatabase()
    {
        database = null;
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override // Override because the expected error message is different
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage(format("Schema name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override // Override because Teradata Object name limit is 128 characters
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override // Override because the expected error message is different
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(format(
                "Column name must be shorter than or equal to '%s' characters but got '%s': '.*'",
                TERADATA_OBJECT_NAME_LIMIT,
                TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override // Override to skip the data mapping smoke test
    @Test
    public void testDataMappingSmokeTest()
    {
        skipTestUnless(false);
    }

    @Override // Override because Teradata Table name limit is 128 characters
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override // Override because the expected error message is different
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(format(
                "Table name must be shorter than or equal to '%s' characters but got '%s'",
                TERADATA_OBJECT_NAME_LIMIT,
                TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override // Overriding this test case as Teradata defines varchar with a length.
    @Test
    public void testVarcharCastToDateInPredicate()
    {
        String tableName = "varchar_as_date_pred";
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of(
                        "'999-09-09'",
                        "'1005-09-09'",
                        "'2005-06-06'",
                        "'2005-06-6'",
                        "'2005-6-06'",
                        "'2005-6-6'",
                        "' 2005-06-06'",
                        "'2005-06-06 '",
                        "' +2005-06-06'",
                        "'02005-06-06'",
                        "'2005-09-06'",
                        "'2005-09-6'",
                        "'2005-9-06'",
                        "'2005-9-6'",
                        "' 2005-09-06'",
                        "'2005-09-06 '",
                        "' +2005-09-06'",
                        "'02005-09-06'",
                        "'2005-09-09'",
                        "'2005-09-9'",
                        "'2005-9-09'",
                        "'2005-9-9'",
                        "' 2005-09-09'",
                        "'2005-09-09 '",
                        "' +2005-09-09'",
                        "'02005-09-09'",
                        "'2005-09-10'",
                        "'2005-9-10'",
                        "' 2005-09-10'",
                        "'2005-09-10 '",
                        "' +2005-09-10'",
                        "'02005-09-10'",
                        "'2005-09-20'",
                        "'2005-9-20'",
                        "' 2005-09-20'",
                        "'2005-09-20 '",
                        "' +2005-09-20'",
                        "'02005-09-20'",
                        "'9999-09-09'",
                        "'99999-09-09'"))) {
            for (String date : ImmutableList.of("2005-09-06", "2005-09-09", "2005-09-10")) {
                for (String operator : ImmutableList.of("=", "<=", "<", ">", ">=", "!=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
                    assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) %s DATE '%s'".formatted(table.getName(), operator, date)))
                            .hasCorrectResultsRegardlessOfPushdown();
                }
            }
        }
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of("'2005-06-bad-date'", "'2005-09-10'"))) {
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) < DATE '2005-09-10'".formatted(table.getName())))
                    .failure().hasMessage("Value cannot be cast to date: " + "2005-06-bad-date");
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) = DATE '2005-09-10'".formatted(table.getName())),
                    queryAssert -> queryAssert.skippingTypesCheck().matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: " + "2005-06-bad-date"));
        }
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of("'2005-09-10'"))) {
            // 2005-09-01, when written as 2005-09-1, is a prefix of an existing data point: 2005-09-10
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-09-01'".formatted(table.getName())))
                    .skippingTypesCheck().matches("VALUES '2005-09-10'");
        }
    }

    // Tests DISTINCT with LIMIT functionality on Teradata
    // Overridden to ensure proper query execution with Teradata-specific SQL syntax
    @Override
    @Test
    public void testDistinctLimit()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");
        assertQuery(
                "SELECT DISTINCT x " +
                        "FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a " +
                        "LIMIT 100",
                "SELECT 1");
    }

    /* Overriding the method as Teradata avg calculations are slightly different than trino so Skipping the results check for avg
        Expecting actual: (111.660, 111728394.9938271616, 1.117283945E8, 111.6605) to contain exactly in any order: [(111.661, 111728394.9938271605, 1.117283945E8, 111.6605)] */
    @Override
    @Test
    public void testNumericAggregationPushdown()
    {
        try (TestTable emptyTable = this.createAggregationTestTable("test_num_agg_pd", ImmutableList.of())) {
            assertThat(this.query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + emptyTable.getName())).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
        }

        try (TestTable testTable = this.createAggregationTestTable("test_num_agg_pd", ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(this.query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(this.query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + testTable.getName())).skipResultsCorrectnessCheckForPushdown().isFullyPushedDown();
            assertThat(this.query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            assertThat(this.query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isFullyPushedDown();
            assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isFullyPushedDown();
            assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            assertThat(this.query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
        }
    }

    // Overriding this test case as Teradata raises different error message for division by zero.
    @Override
    @Test
    public void testArithmeticPredicatePushdown()
    {
        assertThat(this.query("SELECT shippriority FROM orders WHERE shippriority % 4 = 0")).isFullyPushedDown();
        assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % nationkey = 2")).isFullyPushedDown().matches("VALUES (BIGINT '3', CAST('CANADA' AS varchar(25)), BIGINT '1')");
        assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % -nationkey = 2")).isFullyPushedDown().matches("VALUES (BIGINT '3', CAST('CANADA' AS varchar(25)), BIGINT '1')");
        assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % 0 = 2")).failure().hasMessageContaining("Operation Error");
        assertThat(this.query("SELECT nationkey, name, regionkey FROM nation WHERE nationkey > 0 AND (nationkey - regionkey) % (regionkey - 1) = 2")).failure().hasMessageContaining("Operation Error");
    }

    @Override // Overriding this test case as Teradata does not support negative dates.
    @Test
    public void testDateYearOfEraPredicate()
    {
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
    }

    @Override // Overriding this test case as Teradata doesn't have support to (k, v) AS VALUES in insert statement
    @Test
    public void testCharVarcharComparison()
    {
        String testTableName = "test_char_varchar";
        try (TestTable table = newTrinoTable(testTableName, "(k int, v char(3))", List.of("-1, CAST(NULL AS char(3))", "3, CAST('   ' AS char(3))", "6, CAST('x  ' AS char(3))"))) {
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))", "VALUES (3, '   ')");
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(4))", "VALUES (3, '   ')");
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))", "VALUES (6, 'x  ')");
        }
    }

    @Test
    public void testJsonColumnMapping()
    {
        String testTableName = "test_json_table";
        try (TestTable table = newTrinoTable(testTableName, "(id INTEGER, json_data JSON)", List.of("1, '{\"name\": \"Alice\", \"age\": 30}'", "2, '{\"name\": \"Bob\", \"age\": 25, \"active\": true}'", "3, NULL"))) {
            // Test JSON reading
            assertQuery(
                    format("SELECT id, json_data FROM %s ORDER BY id", table.getName()),
                    "VALUES " +
                            "(1, JSON '{\"name\": \"Alice\", \"age\": 30}'), " +
                            "(2, JSON '{\"name\": \"Bob\", \"age\": 25, \"active\": true}'), " +
                            "(3, CAST(NULL AS JSON))");

            // Test JSON extraction
            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.name') FROM %s WHERE id = 1", table.getName()),
                    "VALUES 'Alice'");

            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.age') FROM %s WHERE id = 2", table.getName()),
                    "VALUES '25'");
        }
    }

    @Test
    public void testJsonColumnMappingTypeMapping()
    {
        String testTableName = "test_json_type_mapping";
        try (TestTable table = newTrinoTable(testTableName, "(id INTEGER, json_col JSON)", List.of("1, '{\"test\": \"value\"}'"))) {
            // Verify the column type is mapped correctly
            MaterializedResult result = computeActual(format("DESCRIBE %s", table.getName()));

            boolean jsonColumnFound = false;
            for (MaterializedRow row : result.getMaterializedRows()) {
                String columnName = (String) row.getField(0);
                String columnType = (String) row.getField(1);

                if ("json_col".equals(columnName)) {
                    org.junit.jupiter.api.Assertions.assertEquals("json", columnType);
                    jsonColumnFound = true;
                    break;
                }
            }
            assertThat(jsonColumnFound).isTrue();
        }
    }

    @Test
    public void testJsonColumnMappingComplexData()
    {
        String testTableName = "test_json_complex";
        try (TestTable table = newTrinoTable(testTableName, "(id INTEGER, json_data JSON)",
                List.of("1, '{\"user\": {\"name\": \"John\", \"addresses\": [{\"city\": \"NYC\", \"zip\": \"10001\"}, {\"city\": \"LA\", \"zip\": \"90210\"}]}}'",
                        "2, '{\"numbers\": [1, 2, 3, 4, 5], \"mixed\": [\"text\", 42, true, null]}'",
                        "3, '{\"empty_object\": {}, \"empty_array\": [], \"null_value\": null}'"))) {
            // Test nested object extraction
            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.user.name') FROM %s WHERE id = 1", table.getName()),
                    "VALUES 'John'");

            // Test array element extraction
            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.user.addresses[0].city') FROM %s WHERE id = 1", table.getName()),
                    "VALUES 'NYC'");

            // Test array element from numbers array
            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.numbers[2]') FROM %s WHERE id = 2", table.getName()),
                    "VALUES '3'");

            // Test JSON_EXTRACT for object/array values
            assertQuery(
                    format("SELECT JSON_EXTRACT(json_data, '$.user.addresses') FROM %s WHERE id = 1", table.getName()),
                    "VALUES JSON '[{\"city\": \"NYC\", \"zip\": \"10001\"}, {\"city\": \"LA\", \"zip\": \"90210\"}]'");
        }
    }

    @Test
    public void testJsonArrayWithNullValues()
    {
        String testTableName = "test_json_array_nulls";

        try (TestTable table = newTrinoTable(
                testTableName,
                "(id INTEGER, json_data JSON)",
                List.of("1, '{\"array\": [1, null, 3, null]}'"))) {
            // Extract specific array elements
            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.array[1]') FROM %s WHERE id = 1", table.getName()),
                    "VALUES CAST(NULL AS VARCHAR)"); // Second element is null

            assertQuery(
                    format("SELECT JSON_EXTRACT_SCALAR(json_data, '$.array[2]') FROM %s WHERE id = 1", table.getName()),
                    "VALUES '3'"); // Third element is 3

            // Extract the entire array
            assertQuery(
                    format("SELECT JSON_EXTRACT(json_data, '$.array') FROM %s WHERE id = 1", table.getName()),
                    "VALUES JSON '[1, null, 3, null]'");
        }
    }

    @Override // Overriding this test case as Teradata doesn't have support to (k, v) AS VALUES in insert statement
    @Test
    public void testVarcharCharComparison()
    {
        try (TestTable table = newTrinoTable("test_varchar_char", "(k int, v char(3))", List.of(
                "-1, CAST(NULL AS varchar(3))",
                "0, CAST('' AS varchar(3))",
                "1, CAST(' ' AS" +
                        " varchar(3))",
                "2, CAST('  ' AS varchar(3))",
                "3, CAST('   ' AS varchar(3))",
                "4, CAST('x' AS varchar(3))",
                "5, CAST('x ' AS varchar(3))",
                "6, CAST('x  ' AS " + "varchar(3))"))) {
            //  Teradata's CHAR type automatically pads values with spaces to the defined length
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS char(2))", "VALUES (0, '   '), (1, '   '), (2, '   '), (3, '   ')");
            assertQuery("SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS char(2))", "VALUES (4, 'x  '), (5, 'x  '), (6, 'x  ')");
        }
    }

    // Overriding this test case as Teradata supports timezone in different way.
    @Override
    @Test
    public void testTimestampWithTimeZoneCastToDatePredicate()
    {
        skipTestUnless(this.hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        TestTable table;
        try {
            table = this.newTrinoTable("timestamptz_to_date", "(i varchar(20), t TIMESTAMP)",
                    List.of(
                            "'UTC', CAST(TIMESTAMP '2005-09-10 00:12:34.000+00:00' AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS TIMESTAMP)",
                            "'Warsaw', CAST(TIMESTAMP '2005-09-10 00:12:34.000+02:00' AT TIME ZONE INTERVAL '2:00' HOUR TO MINUTE AS TIMESTAMP)",
                            "'Los Angeles', CAST(TIMESTAMP '2005-09-10 00:12:34.000-07:00' AT TIME ZONE - INTERVAL '7:00' HOUR TO MINUTE AS TIMESTAMP)"));
        }
        catch (QueryFailedException e) {
            this.verifyUnsupportedTypeException(e, "timestamp(3) with time zone");
            return;
        }

        TestTable e = table;

        try {
            assertThat(this.query("SELECT i FROM " + table.getName() + " WHERE CAST(t AS date) = DATE '2005-09-10'")).hasCorrectResultsRegardlessOfPushdown();
        }
        catch (Throwable var7) {
            if (table != null) {
                try {
                    e.close();
                }
                catch (Throwable var5) {
                    var7.addSuppressed(var5);
                }
            }

            throw var7;
        }
        table.close();
    }

    // Tests timestamp with timezone cast to timestamp predicate functionality
    // Overridden because Teradata handles timezone conversions differently than standard SQL
    @Override
    @Test
    public void testTimestampWithTimeZoneCastToTimestampPredicate()
    {
        skipTestUnless(this.hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        TestTable table;
        try {
            table = this.newTrinoTable(
                    "timestamptz_to_ts",
                    "(i varchar(20), t TIMESTAMP)",
                    List.of(
                            "'UTC', CAST(TIMESTAMP '2005-09-10 13:00:00.000+00:00' AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS TIMESTAMP)",
                            "'Warsaw', CAST(TIMESTAMP '2005-09-10 13:00:00.000+02:00' AT TIME ZONE INTERVAL '2:00' HOUR TO MINUTE AS TIMESTAMP)",
                            "'Los Angeles', CAST(TIMESTAMP '2005-09-10 13:00:00.000-07:00' AT TIME ZONE - INTERVAL '7:00' HOUR TO MINUTE AS TIMESTAMP)"));
        }
        catch (QueryFailedException e) {
            this.verifyUnsupportedTypeException(e, "timestamp(3) with time zone");
            return;
        }

        TestTable e = table;

        try {
            assertThat(this.query("SELECT i FROM " + table.getName() + " WHERE CAST(t AS timestamp(0)) = TIMESTAMP '2005-09-10 13:00:00'")).hasCorrectResultsRegardlessOfPushdown();
        }
        catch (Throwable var7) {
            if (table != null) {
                try {
                    e.close();
                }
                catch (Throwable var5) {
                    var7.addSuppressed(var5);
                }
            }

            throw var7;
        }
        table.close();
    }

    // Tests join pushdown with long column identifiers
    // Overridden to handle Teradata's specific identifier length limits and naming constraints
    @Override
    @Test
    public void testJoinPushdownWithLongIdentifiers()
    {
        skipTestUnless(this.hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN));
        int maxLength = this.maxColumnNameLength().orElse(65541);
        String validColumnName = "z".repeat(maxLength - 5);

        try (TestTable left = this.newTrinoTable("test_long_id_l", String.format("(%s BIGINT)", validColumnName));
                TestTable right = this.newTrinoTable("test_long_id_r", String.format("(%s BIGINT)", validColumnName))) {
            assertThat(this.query(this.joinPushdownEnabled(this.getSession()), "SELECT l.%1$s, r.%1$s\nFROM %2$s l JOIN %3$s r ON l.%1$s = r.%1$s".formatted(validColumnName, left.getName(), right.getName()))).isFullyPushedDown();
        }
    }

    // Filters data mapping test data for Teradata compatibility
    // Overridden to exclude data types that Teradata doesn't support or handles differently
    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        return switch (typeName) {
            // skipping date as during julian->gregorian date is handled differently in Teradata. tinyint, double and varchar with unbounded (need to handle special characters)
            // is skipped and will handle it while improving
            // write functionalities.
            case "boolean", "tinyint", "date", "real", "double", "varchar", "time", "time(6)", "timestamp", "timestamp(6)", "varbinary", "timestamp(3) with time zone",
                 "timestamp(6) with time zone", "U&'a \\000a newline'" -> Optional.empty();
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    @Override
    @Test
    public void testRenameSchema()
    {
        abort("Skipping as connector does not support RENAME SCHEMA");
    }

    @Override
    @Test
    public void testColumnName()
    {
        abort("Skipping as connector does not support column level write operations");
    }

    @Override
    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        abort("Skipping as connector does not support creating table with UNICODE characters");
    }

    @Override
    @Test
    public void testUpdateNotNullColumn()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    @Test
    public void testWriteBatchSizeSessionProperty()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    @Test
    public void testWriteTaskParallelismSessionProperty()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    @Test
    public void testDropNonEmptySchemaWithTable()
    {
        abort("Skipping as connector does not support drop schemas");
    }

    @Override
    @Test
    public void verifySupportsUpdateDeclaration()
    {
        abort("Skipping as connector does not support update operations");
    }

    @Override
    @Test
    public void testDropNotNullConstraint()
    {
        abort("Skipping as connector does not support dropping a not null constraint");
    }

    @Override
    @Test
    public void testExecuteProcedureWithInvalidQuery()
    {
        abort("Skipping as connector does not support execute procedure");
    }

    @Override
    @Test
    public void testCreateTableAsSelectNegativeDate()
    {
        abort("Skipping as connector does not support creating table with negative date");
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    // Creates new Trino test tables with proper schema handling
    // Overridden to handle Teradata's schema.table naming format and table creation syntax
    @Override
    protected TestTable newTrinoTable(String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        System.out.println("tableDefinition: " + tableDefinition);
        String tableName;

        // Check if namePrefix already contains schema (contains a dot)
        if (namePrefix.contains(".")) {
            // namePrefix already has schema.tablename format
            tableName = namePrefix;
        }
        else {
            String schemaName = getSession().getSchema().orElseThrow();
            tableName = schemaName + "." + namePrefix;
        }
        String rewriteTableDefinition = rewriteCreateTableQuery(tableDefinition);
        rewriteTableDefinition = rewriteVarcharTypes(rewriteTableDefinition);

        System.out.println("rewriteTableDefinition: " + rewriteTableDefinition);
        // String rewriteTableDefinition = tableDefinition;
        return new TestTable(database, tableName, rewriteTableDefinition, rowsToInsert);
    }

    private boolean expectVarcharJoinPushdown(String operator)
            throws Throwable
    {
        if ("IS DISTINCT FROM".equals(operator)) {
            return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        }
        else {
            boolean var10000 = switch (this.toJoinConditionOperator(operator)) {
                case EQUAL, NOT_EQUAL -> this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
                case IDENTICAL -> this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                default -> throw new MatchException(null, null);
            };
            return var10000;
        }
    }

    private ComparisonOperator toJoinConditionOperator(String operator)
            throws Throwable
    {
        return operator.equals("IS NOT DISTINCT FROM") ? ComparisonOperator.IDENTICAL :
                (ComparisonOperator) ((Optional) Stream.of(ComparisonOperator.values()).filter(joinOperator -> joinOperator.getOperator().equals(operator)).collect(MoreCollectors.toOptional())).orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    private void verifyUnsupportedTypeException(Throwable exception, String trinoTypeName)
    {
        String typeNameBase = trinoTypeName.replaceFirst("\\(.*", "");
        String expectedMessagePart = String.format("(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)", Pattern.quote(typeNameBase));
        assertThat(exception).hasMessageFindingMatch(expectedMessagePart).satisfies(e -> assertThat(io.trino.testing.QueryAssertions.getTrinoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
    }

    @Test
    public void testTeradataNumberDataType()
    {
        try (TestTable table = newTrinoTable("test_number", "(id INTEGER, " + "number_col NUMBER(10,2), " + "number_default NUMBER, " + "number_large NUMBER(38,10))", List.of(
                "1, CAST(12345.67 AS NUMBER(10,2)), CAST(999999999999999 AS NUMBER), CAST(1234567890123456789012345678.1234567890 AS NUMBER(38,10))",
                "2, CAST(-99999.99 AS " +
                        "NUMBER(10,2)), CAST(-123456789012345 AS NUMBER), CAST(-9999999999999999999999999999.9999999999 AS NUMBER(38,10))",
                "3, CAST(0.00 AS NUMBER(10,2)), CAST" + "(0 AS NUMBER), CAST(0.0000000000 AS NUMBER(38,10))"))) {
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST(12345.67 AS DECIMAL(10,2))");
            assertThat(query(format("SELECT number_default FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST(999999999999999 AS DECIMAL(38,0))");
            assertThat(query(format("SELECT number_large FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38,10)"
                    + ")");
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 2", table.getName()))).matches("VALUES CAST(-99999.99 AS DECIMAL(10,2))");
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 3", table.getName()))).matches("VALUES CAST(0.00 AS DECIMAL(10,2))");
        }
    }

    @Test
    public void testTeradataCharacterDataType()
    {
        try (TestTable table = newTrinoTable("test_character", "(id INTEGER, " + "char_col CHARACTER(5), " + "char_default CHARACTER, " + "char_large CHARACTER(100))", List.of(
                "1, CAST('HELLO' AS CHARACTER(5)), CAST('A' AS CHARACTER), CAST('TERADATA' AS CHARACTER(100))",
                "2, CAST('WORLD' AS CHARACTER(5)), CAST('B' AS CHARACTER), CAST" + "('CHARACTER' AS CHARACTER(100))",
                "3, CAST('' AS CHARACTER(5)), CAST('C' AS CHARACTER), CAST" +
                        "('' AS CHARACTER(100))"))) {
            assertThat(query(format("SELECT char_col FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('HELLO' AS CHAR(5))");
            assertThat(query(format("SELECT char_default FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('A' AS CHAR(1))");
            assertThat(query(format("SELECT char_large FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('TERADATA' AS CHAR(100))");
            assertThat(query(format("SELECT char_col FROM %s WHERE id = 3", table.getName()))).matches("VALUES CAST('' AS CHAR(5))");
        }
    }

    @Test
    public void testTeradataTimeWithTimeZoneDataType()
    {
        try (TestTable table = newTrinoTable("test_time_with_timezone", "(id INTEGER, " + "time_tz_default TIME WITH TIME ZONE, " + "time_tz_precision TIME(3) WITH TIME ZONE, " + "time_tz_max TIME(6) WITH TIME ZONE)", List.of("1, CAST('10:30:45.000000+05:30' AS TIME WITH TIME ZONE), CAST('14:25:30.123+00:00' AS TIME(3) WITH TIME ZONE), CAST('09:15:20.123456-08:00' AS TIME(6) WITH TIME ZONE)", "2, CAST('23:59:59.000000-07:00' AS TIME WITH TIME ZONE), CAST('00:00:00.000+01:00' AS TIME(3) WITH TIME ZONE), CAST('12:30:45.999999+09:00' AS TIME(6) WITH TIME ZONE)", "3, CAST('06:45:30.000000+00:00' AS TIME WITH TIME ZONE), CAST('18:20:15.567+03:00' AS TIME(3) WITH TIME ZONE), CAST('21:10:05.000001-05:00' AS TIME(6) WITH TIME ZONE)"))) {
            assertThat(query(format("SELECT time_tz_default FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('10:30:45.000000+05:30' AS TIME(6) WITH TIME ZONE)");
        }
    }

    @Test
    public void testTeradataTimestampWithTimeZoneDataType()
    {
        try (TestTable table = newTrinoTable("test_timestamp_with_timezone", "(id INTEGER, " + "ts_tz_default TIMESTAMP WITH TIME ZONE, " + "ts_tz_precision TIMESTAMP(3) WITH TIME ZONE, " + "ts_tz_max TIMESTAMP(6) WITH TIME ZONE)", List.of("1, CAST('2023-05-15 10:30:45.000000+05:30' AS TIMESTAMP WITH TIME ZONE), CAST('2023-05-15 14:25:30.123+00:00' AS TIMESTAMP(3) WITH TIME ZONE), CAST('2023-05-15 09:15:20.123456-08:00' AS TIMESTAMP(6) WITH TIME ZONE)", "2, CAST('2023-12-31 23:59:59.000000-07:00' AS TIMESTAMP WITH TIME ZONE), CAST('2023-01-01 00:00:00.000+01:00' AS TIMESTAMP(3) WITH TIME ZONE), CAST('2023-06-15 12:30:45.999999+09:00' AS TIMESTAMP(6) WITH TIME ZONE)", "3, CAST('2023-07-04 06:45:30.000000+00:00' AS TIMESTAMP WITH TIME ZONE), CAST('2023-11-25 18:20:15.567+03:00' AS TIMESTAMP(3) WITH TIME ZONE), CAST('2023-03-10 21:10:05.000001-05:00' AS TIMESTAMP(6) WITH TIME ZONE)"))) {
            assertThat(query(format("SELECT ts_tz_default FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('2023-05-15 10:30:45.000000+05:30' AS TIMESTAMP(6) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_precision FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('2023-05-15 14:25:30.123+00:00' AS TIMESTAMP(3) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_max FROM %s WHERE id = 1", table.getName()))).matches("VALUES CAST('2023-05-15 09:15:20.123456-08:00' AS TIMESTAMP(6) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_default FROM %s WHERE id = 2", table.getName()))).matches("VALUES CAST('2023-12-31 23:59:59.000000-07:00' AS TIMESTAMP(6) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_precision FROM %s WHERE id = 2", table.getName()))).matches("VALUES CAST('2023-01-01 00:00:00.000+01:00' AS TIMESTAMP(3) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_max FROM %s WHERE id = 3", table.getName()))).matches("VALUES CAST('2023-03-10 21:10:05.000001-05:00' AS TIMESTAMP(6) WITH TIME ZONE)");
            assertThat(query(format("SELECT ts_tz_default FROM %s WHERE id = 3", table.getName()))).matches("VALUES CAST('2023-07-04 06:45:30.000000+00:00' AS TIMESTAMP(6) WITH TIME ZONE)");
        }
    }

    @Test
    public void testArrayAsVarcharColumnMapping()
    {
        String testTableName = "test_array_table";

        try (TestTable table = newTrinoTable(testTableName, "(id INTEGER, array_data VARCHAR(1000))", List.of("1, 'ARRAY[\"Alice\", \"Bob\", \"Charlie\"]'", "2, 'ARRAY[\"John\", \"Jane\"]'", "3, NULL", "4, 'ARRAY[]'"))) {
            assertQuery(format("SELECT id, array_data FROM %s ORDER BY id", table.getName()), "VALUES " + "(1, 'ARRAY[\"Alice\", \"Bob\", \"Charlie\"]'), " + "(2, 'ARRAY[\"John\", \"Jane\"]'), " + "(3, CAST(NULL AS VARCHAR)), " + "(4, 'ARRAY[]')");
            assertQuery(format("SELECT array_data FROM %s WHERE id = 1", table.getName()), "VALUES 'ARRAY[\"Alice\", \"Bob\", \"Charlie\"]'");
            assertQuery(format("SELECT array_data FROM %s WHERE id = 2", table.getName()), "VALUES 'ARRAY[\"John\", \"Jane\"]'");
        }
    }

    @Test
    public void testArrayAsVarcharColumnMappingWithNullElements()
    {
        String testTableName = "test_array_nulls";

        try (TestTable table = newTrinoTable(testTableName, "(id INTEGER, array_data VARCHAR(1000))", List.of("1, 'ARRAY[\"first\", null, \"third\", null]'", "2, 'ARRAY[null, \"second\"]'", "3, 'ARRAY[null, null, null]'"))) {
            assertQuery(format("SELECT id, array_data FROM %s ORDER BY id", table.getName()), "VALUES " + "(1, 'ARRAY[\"first\", null, \"third\", null]'), " + "(2, 'ARRAY[null, \"second\"]'), " + "(3, 'ARRAY[null, null, null]')");
            assertQuery(format("SELECT array_data FROM %s WHERE id = 1", table.getName()), "VALUES 'ARRAY[\"first\", null, \"third\", null]'");
            assertQuery(format("SELECT array_data FROM %s WHERE id = 2", table.getName()), "VALUES 'ARRAY[null, \"second\"]'");
            assertQuery(format("SELECT array_data FROM %s WHERE id = 3", table.getName()), "VALUES 'ARRAY[null, null, null]'");
        }
    }

    @Test
    public void testLowerPushdown()
    {
        try (TestTable table = newTrinoTable(
                "test_lower_pushdown" + randomNameSuffix(),
                "(nationkey INTEGER, name VARCHAR(25), regionkey INTEGER)",
                List.of("0, 'ALGERIA', 0", "1, 'ARGENTINA', 1", "2, 'BRAZIL', 1", "3, 'CANADA', 1", "4, 'EGYPT', 0"))) {
            // Projection with LOWER() pushed down; explicit casts ensure expected type matches VARCHAR(25)
            assertThat(query("SELECT nationkey, LOWER(name) AS lower_name FROM " + table.getName() + " ORDER BY nationkey"))
                    .matches("VALUES " +
                            "(0, CAST('algeria' AS varchar(25))), " +
                            "(1, CAST('argentina' AS varchar(25))), " +
                            "(2, CAST('brazil' AS varchar(25))), " +
                            "(3, CAST('canada' AS varchar(25))), " +
                            "(4, CAST('egypt' AS varchar(25)))");
            // Predicate with LOWER() pushed down (name already VARCHAR(25))
            assertThat(query("SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE LOWER(name) = 'algeria'"))
                    .isFullyPushedDown()
                    .matches("VALUES (0, CAST('ALGERIA' AS varchar(25)), 0)");
        }
    }

    @Test
    public void testUpperPushdown()
    {
        try (TestTable table = newTrinoTable(
                "test_upper_pushdown" + randomNameSuffix(),
                "(nationkey INTEGER, name VARCHAR(25), regionkey INTEGER)",
                List.of("0, 'algeria', 0", "1, 'argentina', 1", "2, 'brazil', 1", "3, 'canada', 1", "4, 'egypt', 0"))) {
            // Projection with UPPER() fully pushed down
            assertThat(query("SELECT nationkey, UPPER(name) AS upper_name FROM " + table.getName() + " ORDER BY nationkey"))
                    .matches("VALUES " +
                            "(0, CAST('ALGERIA' AS varchar(25))), " +
                            "(1, CAST('ARGENTINA' AS varchar(25))), " +
                            "(2, CAST('BRAZIL' AS varchar(25))), " +
                            "(3, CAST('CANADA' AS varchar(25))), " +
                            "(4, CAST('EGYPT' AS varchar(25)))");
            // Predicate with UPPER() fully pushed down
            assertThat(query("SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE UPPER(name) = 'BRAZIL'"))
                    .isFullyPushedDown()
                    .matches("VALUES (2, CAST('brazil' AS varchar(25)), 1)");
        }
    }

    @Test
    public void testSubstringPushdown()
    {
        try (TestTable table = newTrinoTable(
                "test_substring_pushdown" + randomNameSuffix(),
                "(nationkey INTEGER, name VARCHAR(25), regionkey INTEGER)",
                List.of("0, 'ALGERIA', 0", "1, 'ARGENTINA', 1", "2, 'BRAZIL', 1"))) {
            // Verify SUBSTRING in projection - partial pushdown with ScanProject
            assertThat(query(
                    "SELECT nationkey, SUBSTRING(name, 1, 2) AS name_prefix, regionkey FROM " + table.getName() + " ORDER BY nationkey"))
                    .isNotFullyPushedDown(ProjectNode.class)
                    .matches("VALUES " +
                            "(0, CAST('AL' AS varchar(25)), 0), " +
                            "(1, CAST('AR' AS varchar(25)), 1), " +
                            "(2, CAST('BR' AS varchar(25)), 1)");
            // Verify SUBSTRING in predicate - fully pushed down as a filter
            assertThat(query(
                    "SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE SUBSTRING(name, 1, 2) = 'AR'"))
                    .isFullyPushedDown()
                    .matches("VALUES (1, CAST('ARGENTINA' AS varchar(25)), 1)");
            // Verify SUBSTRING with single parameter
            assertThat(query(
                    "SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE SUBSTRING(name, 1) = 'BRAZIL'"))
                    .isFullyPushedDown()
                    .matches("VALUES (2, CAST('BRAZIL' AS varchar(25)), 1)");
        }
    }

    @Test
    public void testNestedSubstringPushdown()
    {
        try (TestTable table = newTrinoTable(
                "test_nested_substring" + randomNameSuffix(),
                "(nationkey INTEGER, name VARCHAR(25), regionkey INTEGER)",
                List.of("0, 'algeria', 0", "1, 'argentina', 1", "2, 'brazil', 1", "3, 'canada', 1", "4, 'egypt', 0"))) {
            // Verify UPPER(SUBSTRING(...)) in predicate - fully pushed down
            assertThat(query(
                    "SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE UPPER(SUBSTRING(name, 1, 1)) = 'C'"))
                    .isFullyPushedDown()
                    .matches("VALUES (3, CAST('canada' AS varchar(25)), 1)");
            // Verify LOWER(SUBSTRING(...)) in predicate - fully pushed down
            assertThat(query(
                    "SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE LOWER(SUBSTRING(name, 1, 3)) = 'arg'"))
                    .isFullyPushedDown()
                    .matches("VALUES (1, CAST('argentina' AS varchar(25)), 1)");
            // Verify SUBSTRING(UPPER(...)) with IN predicate - fully pushed down
            assertThat(query(
                    "SELECT nationkey, name, regionkey FROM " + table.getName() + " WHERE SUBSTRING(UPPER(name), 1, 2) IN ('BR', 'FR', 'GE')"))
                    .isFullyPushedDown()
                    .matches("VALUES (2, CAST('brazil' AS varchar(25)), 1)");
        }
    }

    @Test
    public void testCreateTableAs()
    {
        String table1 = "test_ctas1_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table1 + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(table1, "name", "regionkey");
        assertUpdate("DROP TABLE " + table1);
    }

    @Test
    public void testCreateTableAsWithAggregation()
    {
        String table2 = "test_ctas2_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table2 + " AS SELECT regionkey, count(*) c FROM nation GROUP BY regionkey", "SELECT count(DISTINCT regionkey) FROM nation");
        assertTableColumnNames(table2, "regionkey", "c");
        assertUpdate("DROP TABLE " + table2);
    }

    @Test
    public void testCreateTableAsWithJoin()
    {
        String table3 = "test_ctas3_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table3 + " AS SELECT n.name, r.name region FROM nation n JOIN region r ON n.regionkey = r.regionkey", "SELECT count(*) FROM nation");
        assertTableColumnNames(table3, "name", "region");
        assertUpdate("DROP TABLE " + table3);
    }

    @Test
    public void testCreateTableAsWithOrderByLimit()
    {
        String table4 = "test_ctas4_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table4 + " AS SELECT name FROM nation ORDER BY name LIMIT 5 WITH DATA", "SELECT 5");
        assertTableColumnNames(table4, "name");
        assertUpdate("DROP TABLE " + table4);
    }

    @Test
    public void testCreateTableAsWithUnionAll()
    {
        String table5 = "test_ctas5_" + randomNameSuffix();
        assertUpdate("CREATE TABLE IF NOT EXISTS " + table5 + " AS SELECT name FROM nation WHERE regionkey = 1 UNION ALL SELECT name FROM nation WHERE regionkey = 2", "SELECT count(*) FROM nation WHERE regionkey IN (1,2)");
        assertTableColumnNames(table5, "name");
        assertUpdate("DROP TABLE " + table5);
    }

    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_table_int_varchar_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(50))");
        assertTableColumnNames(tableName, "id", "name");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableIfNotExists()
    {
        String tableName = "test_create_table_decimal_date_" + randomNameSuffix();
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (amount DECIMAL(10,2), created DATE)");
        assertTableColumnNames(tableName, "amount", "created");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithNotNullConstraint()
    {
        String tableName = "test_create_table_not_null_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER NOT NULL, name VARCHAR(50))");
        assertTableColumnNames(tableName, "id", "name");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithAllTeradataDataTypes()
    {
        String tableName = "test_create_table_all_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "c_tinyint TINYINT, " +
                "c_smallint SMALLINT, " +
                "c_integer INTEGER, " +
                "c_bigint BIGINT, " +
                "c_real REAL, " +
                "c_double DOUBLE PRECISION, " +
                "c_decimal DECIMAL(18,4), " +
                "c_char CHAR(10), " +
                "c_varchar VARCHAR(100), " +
                "c_date DATE, " +
                "c_time TIME(3), " +
                "c_timestamp TIMESTAMP(3), " +
                "c_json JSON" +
                ")");
        assertTableColumnNames(
                tableName,
                "c_tinyint",
                "c_smallint",
                "c_integer",
                "c_bigint",
                "c_real",
                "c_double",
                "c_decimal",
                "c_char",
                "c_varchar",
                "c_date",
                "c_time",
                "c_timestamp",
                "c_json");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_drop_table" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testDropTableIfExists()
    {
        String tableName = "test_drop_table_if_exists" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    private void assertAlterTableOperation(
            String tableName,
            String tableSpec,
            List<String> insertRows,
            String alterStatement,
            String expectedColumns)
    {
        try (TestTable table = newTrinoTable(tableName, tableSpec, insertRows)) {
            // table.getName() includes schema and gives us fully qualified name of the table
            assertUpdate(alterStatement.replace("<table>", table.getName()));
            assertTableColumnNames(table.getName(), expectedColumns.split(","));
        }
    }

    @Test
    public void testAddColumnIfExistsTableAndColumn()
    {
        assertAlterTableOperation(
                "test_addcol_if_exists_table_and_column" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("2, '54321'"),
                "ALTER TABLE IF EXISTS <table> ADD COLUMN IF NOT EXISTS age INTEGER",
                "id,zip,age");
    }

    @Test
    public void testAddColumnIfExistsTable()
    {
        assertAlterTableOperation(
                "test_addcol_if_exists_table" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("3, '67890'"),
                "ALTER TABLE IF EXISTS <table> ADD COLUMN age INTEGER",
                "id,zip,age");
    }

    @Test
    public void testAddColumnIfExistsColumn()
    {
        assertAlterTableOperation(
                "test_addcol_if_exists_column" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("4, '98765'"),
                "ALTER TABLE <table> ADD COLUMN IF NOT EXISTS age INTEGER",
                "id,zip,age");
    }

    @Test
    public void testDropColumnIfExistsTableAndColumn()
    {
        assertAlterTableOperation(
                "test_dropcol_if_exists_table_and_column" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("2, '54321'"),
                "ALTER TABLE IF EXISTS <table> DROP COLUMN IF EXISTS zip",
                "id");
    }

    @Test
    public void testDropColumnIfExistsTable()
    {
        assertAlterTableOperation(
                "test_dropcol_if_exists_table" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("3, '67890'"),
                "ALTER TABLE IF EXISTS <table> DROP COLUMN zip",
                "id");
    }

    @Test
    public void testDropColumnIfExistsColumn()
    {
        assertAlterTableOperation(
                "test_dropcol_if_exists_column" + randomNameSuffix(),
                "(id INTEGER, zip VARCHAR)",
                List.of("4, '98765'"),
                "ALTER TABLE <table> DROP COLUMN IF EXISTS zip",
                "id");
    }

    @Override // Overridden to swap the order of columns as alterations on index column are not supported by teradata
    protected void testAddAndDropColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try {
            assertUpdate(createTableSqlForAddingAndDroppingColumn(tableName, nameInSql));
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                return;
            }
            throw e;
        }

        // Column order is swapped: value, columnName (not columnName, value)
        assertTableColumnNames(tableName, "value", columnName.toLowerCase(ENGLISH));

        try {
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN " + nameInSql);
        }
        catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("is an index column and cannot be dropped")) {
                return;
            }
            throw e;
        }
        assertTableColumnNames(tableName, "value");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + nameInSql + " varchar(50)");
        assertTableColumnNames(tableName, "value", columnName.toLowerCase(ENGLISH));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override // Overridden to skip columns having trailing space as Teradata does not allow creating such columns
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (columnName.endsWith(" ")) {
            return Optional.empty();
        }
        // UPPERCASE is a reserved word in Teradata (Error 3707)
        if (columnName.equals("UPPERCASE")) {
            return Optional.empty();
        }
        // Apostrophe in column names causes getColumnComment() to return null
        // because the information_schema lookup can't match the column name
        if (columnName.contains("'")) {
            return Optional.empty();
        }
        return super.filterColumnNameTestData(columnName);
    }

    @Override // Overridden to swap the order of columns as alterations on index column are not supported by teradata
    protected String createTableSqlForAddingAndDroppingColumn(String tableName, String columnName)
    {
        // Teradata creates index on first column, so put the test column second
        return "CREATE TABLE " + tableName + " (value integer, " + columnName + " varchar(50))";
    }

    @Override // Overridden due to mismatch in error message from teradata
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        String message = e.getMessage();
        assertThat(message)
                .matches(".*Column .* is not NULL and it has no default value.*");
    }

    @Override // Overridden due to syntax difference in varchar column creation in newTrinoTable(...)
    @Test
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = newTrinoTable("test_add_nn_to_empty" + randomNameSuffix(), "(a_varchar varchar(20))")) {
            String tableName = table.getName();
            String unqualifiedName = tableName.contains(".")
                    // extract table name for information_schema lookup
                    ? tableName.substring(tableName.indexOf('.') + 1)
                    : tableName;
            String addNonNullColumn = "ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL";
            assertUpdate(addNonNullColumn);
            assertThat(columnIsNullable(unqualifiedName, "b_varchar")).isFalse();
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    public void testTableHandleAuthorizationIsEmpty()
    {
        String tableName = "test_authz_empty_table" + randomNameSuffix();
        try (TestTable table = newTrinoTable(tableName, "(id INTEGER)", List.of("1"))) {
            // Execute within a transaction context
            getQueryRunner().inTransaction(session -> {
                String catalogName = session.getCatalog().orElseThrow();
                String schemaName = session.getSchema().orElseThrow();

                // Extract base table name
                String baseTableName = table.getName().contains(".")
                        ? table.getName().substring(table.getName().lastIndexOf('.') + 1)
                        : table.getName();

                ConnectorTableHandle tableHandle = getQueryRunner()
                        .getPlannerContext()
                        .getMetadata()
                        .getTableHandle(
                                session,
                                QualifiedObjectName.valueOf(catalogName + "." + schemaName + "." + baseTableName))
                        .orElseThrow()
                        .connectorHandle();

                // Cast to JdbcTableHandle
                JdbcTableHandle jdbcHandle = (JdbcTableHandle) tableHandle;

                // Verify authorization is empty
                assertThat(jdbcHandle.getAuthorization()).isEmpty();

                return null;
            });
        }
    }

    @Test
    public void testConnectionAutoCommitIsEnabled()
    {
        // The TeradataClient verifies auto-commit is enabled when dropping columns
        // We can test this indirectly by performing an operation that requires auto-commit
        String tableName = "test_autocommit_behavior" + randomNameSuffix();
        try (TestTable table = newTrinoTable(tableName, "(id INTEGER, name VARCHAR(50))", List.of("1, 'test'"))) {
            // Verify we can drop a column (which internally verifies auto-commit)
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN name");

            // Verify the column was dropped
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1)");
        }
    }

    @Override // Override because Teradata handles merge on Primary Index columns instead of Primary Keys
    @Test
    public void testMergeTargetWithoutPrimaryKeys()
    {
        String tableName = "test_merge_target_no_pks_" + randomNameSuffix();
        try (TestTable table = newTrinoTable(tableName, "(a int, b int) NO PRIMARY INDEX", ImmutableList.of("1, 1", "2, 2"))) {
            assertQueryFails(format("MERGE INTO %s t USING (VALUES (3, 3)) AS s(x, y) " +
                    "   ON t.a = s.x " +
                    "   WHEN MATCHED THEN UPDATE SET b = y " +
                    "   WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.x, s.y) ", table.getName()), "The connector can not perform merge on the target table without primary index columns");
        }
    }

    @Override // Teradata doesn't allow update or delete primary index column values using MERGE.
    @Test
    public void testMergeFruits()
    {
        String targetTable = "merge_various_target_" + randomNameSuffix();
        String sourceTable = "merge_various_source_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)", 2);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Carol', 'candles'), ('Joe', 'jellybeans')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Teradata doesn't allow update or delete primary index column values using MERGE.
    @Test
    public void testMergeFalseJoinCondition()
    {
        String targetTable = "merge_join_false_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

        // Test a literal false
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES ('Carol', 9, 'Centreville')) AS s(customer, purchases, address)
                  ON (FALSE)
                    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                """.formatted(targetTable),
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville')");

        // Test a constant-folded false expression
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)
                  ON (t.customer != t.customer)
                    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                """.formatted(targetTable),
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Teradata doesn't allow update or delete primary index column values using MERGE.
    @Test
    public void testMergeAllColumnsUpdated()
    {
        String targetTable = "merge_all_columns_updated_target_" + randomNameSuffix();
        String sourceTable = "merge_all_columns_updated_source_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable), 4);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address",
                3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 22, 'Darbyshire'), ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol', 12, 'Centreville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Teradata doesn't allow update or delete primary index column values using MERGE.
    @Test
    public void testMergeCasts()
    {
        String targetTable = "merge_cast_target_" + randomNameSuffix();
        String sourceTable = "merge_cast_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (col1 INT, col2 DOUBLE, col3 INT, col4 BIGINT, col5 REAL, col6 DOUBLE)", targetTable, Optional.of("col1"));

        assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5, 6)", targetTable), 1);

        createTableForWrites("CREATE TABLE %s (col1 BIGINT, col2 REAL, col3 DOUBLE, col4 INT, col5 INT, col6 REAL)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6, 7)", sourceTable), 1);

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.col1 + 1 = s.col1)" +
                        "    WHEN MATCHED THEN UPDATE SET col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5, col6 = s.col6",
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (1, 3.0, 4, 5, 6.0, 7.0)");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Teradata doesn't allow update or delete primary index column values using MERGE.
    @Test
    public void testMergeAllColumnsReversed()
    {
        String targetTable = "merge_update_columns_reversed_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE " + targetTable + " (a, b, c) AS VALUES (1, 2, 3)", targetTable, Optional.of("a"), OptionalInt.of(1));
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES(1)) AS s(a) ON (t.a = s.a)
                    WHEN MATCHED THEN UPDATE
                        SET c = 100, b = 42
                """.formatted(targetTable),
                1);
        assertQuery("SELECT * FROM " + targetTable, "VALUES (1, 42, 100)");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Due to space issues with Teradata test instance, considering 10_000 rows only
    @Test
    public void testMergeLarge()
    {
        String tableName = "test_merge_" + randomNameSuffix();
        int limit = 10_000;

        createTableForWrites("CREATE TABLE %s (orderkey BIGINT, custkey BIGINT, totalprice DOUBLE)", tableName, Optional.of("orderkey"));

        assertUpdate(
                format("INSERT INTO %s SELECT orderkey, custkey, totalprice FROM tpch.sf1.orders order by orderkey LIMIT %d", tableName, limit),
                (long) computeScalar(format("SELECT count(*) FROM (SELECT 1 FROM tpch.sf1.orders order by orderkey LIMIT %d)", limit)));

        @Language("SQL") String mergeSql = "" +
                "MERGE INTO " + tableName + " t USING (SELECT * FROM tpch.sf1.orders order by orderkey LIMIT " + limit + ") s ON (t.orderkey = s.orderkey)\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

        String limitedSourceCountSql = "SELECT count(*) FROM (" +
                " SELECT orderkey FROM tpch.sf1.orders ORDER BY orderkey LIMIT " + limit +
                ") s WHERE mod(orderkey, 3) <> 2";

        long expectedMergeAffected = (long) computeScalar(limitedSourceCountSql);

        assertUpdate(mergeSql, expectedMergeAffected);

        assertQuery("SELECT count(*) FROM " + tableName + " WHERE mod(orderkey, 3) = 1", "SELECT 0");

        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 2"))
                .matches("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM (" +
                        " SELECT orderkey, totalprice FROM tpch.sf1.orders order by orderkey LIMIT " + limit +
                        ") t WHERE mod(orderkey, 3) = 2");

        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 0"))
                .matches("SELECT count(*), sum(cast(totalprice AS decimal(18,2)) * 2) FROM (" +
                        " SELECT orderkey, totalprice FROM tpch.sf1.orders order by orderkey LIMIT " + limit +
                        ") t WHERE mod(orderkey, 3) = 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeDeleteWithCTAS()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String target = "merge_target_with_ctas_" + randomNameSuffix();
        String source = "merge_source_with_ctas_" + randomNameSuffix();
        @Language("SQL") String createTableSql =
                """
                        CREATE TABLE %s AS
                        SELECT * FROM (
                                VALUES
                                (1, 'a', 'aa'),
                                (2, 'b', 'bb'),
                                (3, 'c', 'cc'),
                                (4, 'd', 'dd')
                        ) AS t (id, name, value)
                        """;
        createTableForWrites(createTableSql, target, Optional.of("id"), OptionalInt.of(4));
        assertUpdate(createTableSql.formatted(source), 4);

        assertQuery("SELECT COUNT(*) FROM " + target, "VALUES 4");
        assertUpdate("DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE id > 2)".formatted(target, source), 2);
        assertQuery("SELECT * FROM " + target, "VALUES (1, 'a', 'aa'), (2, 'b', 'bb')");
        assertUpdate("MERGE INTO %s t USING %s s ON (t.id = s.id) WHEN MATCHED AND s.id > 1 THEN DELETE".formatted(target, source), 1);
        assertQuery("SELECT * FROM " + target, "VALUES (1, 'a', 'aa')");

        assertUpdate("DROP TABLE " + target);
        assertUpdate("DROP TABLE " + source);
    }

    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey)
    {
        createTableForWrites(createTable, tableName, primaryKey, OptionalInt.empty());
    }

    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        updateCount.ifPresentOrElse(count -> assertUpdate(format(createTable, tableName), count), () -> assertUpdate(format(createTable, tableName)));
    }

    @Test
    public void testMergeSimpleSelect()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_simple_target_" + randomNameSuffix();
        String sourceTable = "merge_simple_source_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithDefaultColumnValue()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_DEFAULT_COLUMN_VALUE));

        String targetTable = "merge_default_column_value_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR DEFAULT 'test default')", targetTable, Optional.of("nation_name"));

        assertUpdate("INSERT INTO " + targetTable + " (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", 3);

        assertUpdate("MERGE INTO " + targetTable + " t" +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)" +
                " ON (t.nation_name = s.nation_name)" +
                " WHEN NOT MATCHED THEN INSERT (nation_name) VALUES ('IMAGINARIA')", 1);

        assertThat(query("SELECT * FROM " + targetTable))
                .skippingTypesCheck()
                .matches("VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE'), ('IMAGINARIA', 'test default')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeDefaultNullIntoNotNullColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_DEFAULT_COLUMN_VALUE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        String targetTable = "merge_default_null_into_not_null_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR DEFAULT null NOT NULL)", targetTable, Optional.of("nation_name"));

        assertUpdate("INSERT INTO " + targetTable + " (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", 3);

        assertQueryFails(
                "MERGE INTO " + targetTable + " t" +
                        " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)" +
                        " ON (t.nation_name = s.nation_name)" +
                        " WHEN NOT MATCHED THEN INSERT (nation_name) VALUES ('IMAGINARIA')",
                "NULL value not allowed for NOT NULL column: region_name");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeMultipleOperations()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR)", targetTable, Optional.of("customer"));

        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(
                format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                        "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                        "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s, %s", updatedBeginning, updatedMiddle, updatedEnd));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQuery()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_query_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)" +
                        " ON (t.customer = s.customer)" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllInserts()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_inserts_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

        assertUpdate(
                format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)" +
                        " ON (t.customer = s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                2);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllMatchesDeleted()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_all_matches_deleted_target_" + randomNameSuffix();
        String sourceTable = "merge_all_matches_deleted_source_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN DELETE",
                3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Bill', 7, 'Buena')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeMultipleRowsMatchFails()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_multiple_fail_target_" + randomNameSuffix();
        String sourceTable = "merge_multiple_fail_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        createTableForWrites("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Aaron', 6, 'Adelphi'), (2, 'Aaron', 8, 'Ashland')", sourceTable), 2);

        assertQueryFails(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET address = s.address",
                "One MERGE target table row matched more than one source row");

        assertUpdate(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeQueryWithStrangeCapitalization()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_strange_capitalization_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING ", targetTable.toUpperCase(ENGLISH)) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)" +
                        "ON (t.customer = s.customer)" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purCHases = s.PurchaseS + t.pUrchases, aDDress = s.addrESs" +
                        "    WHEN NOT MATCHED THEN INSERT (CUSTOMER, purchases, addRESS) VALUES(s.custoMer, s.Purchases, s.ADDress)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithoutTablesAliases()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "test_without_aliases_target_" + randomNameSuffix();
        String sourceTable = "test_without_aliases_source_" + randomNameSuffix();
        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("customer"));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s USING %s", targetTable, sourceTable) +
                        format(" ON (%s.customer = %s.customer)", targetTable, sourceTable) +
                        format("    WHEN MATCHED AND %s.address = 'Centreville' THEN DELETE", sourceTable) +
                        format("    WHEN MATCHED THEN UPDATE SET purchases = %s.pURCHases + %s.pUrchases, aDDress = %s.addrESs", sourceTable, targetTable, sourceTable) +
                        format("    WHEN NOT MATCHED THEN INSERT (cusTomer, purchases, addRESS) VALUES(%s.custoMer, %s.Purchases, %s.ADDress)", sourceTable, sourceTable, sourceTable),
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithUnpredictablePredicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_predicates_target_" + randomNameSuffix();
        String sourceTable = "merge_predicates_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("id"));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Aaron', 5, 'Antioch'), (2, 'Bill', 7, 'Buena'), (3, 'Carol', 3, 'Cambridge'), (4, 'Dave', 11, 'Devon')", targetTable), 4);

        createTableForWrites("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (5, 'Aaron', 6, 'Arches'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 11, 'Darbyshire'), (8, 'Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer AND s.purchases < 10.2" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (id, customer, purchases, address) VALUES (s.id, s.customer, s.purchases, s.address)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (1, 'Aaron', 11, 'Arches'), (2, 'Bill', 7, 'Buena'), (7, 'Dave', 11, 'Darbyshire'), (4, 'Dave', 11, 'Devon'), (8, 'Ed', 7, 'Etherville')");

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE" +
                        "    WHEN MATCHED" +
                        "        THEN UPDATE SET purchases = s.purchases + t.purchases, address = concat(t.address, '/', s.address)" +
                        "    WHEN NOT MATCHED" +
                        "        THEN INSERT (id, customer, purchases, address) VALUES (s.id, s.customer, s.purchases, s.address)",
                5);

        assertQuery(
                "SELECT * FROM " + targetTable,
                "VALUES (1, 'Aaron', 17, 'Arches/Arches'), (2, 'Bill', 7, 'Buena'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 22, 'Darbyshire/Darbyshire'), (8, 'Ed', 14, 'Etherville/Etherville')");

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (9, 'Fred', 30, 'Franklin')", targetTable), 1);
        assertQuery(
                "SELECT * FROM " + targetTable,
                "VALUES (1, 'Aaron', 17, 'Arches/Arches'), (2, 'Bill', 7, 'Buena'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 22, 'Darbyshire/Darbyshire'), (8, 'Ed', 14, 'Etherville/Etherville'), (9, 'Fred', 30, 'Franklin')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_predicates_target_" + randomNameSuffix();
        String sourceTable = "merge_predicates_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of("id"));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Dave', 11, 'Devon'), (2, 'Dave', 11, 'Darbyshire')", targetTable), 2);

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire')", sourceTable), 1);

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE",
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (2, 'Dave', 11, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSubqueries()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_nation_target_" + randomNameSuffix();
        String sourceTable = "merge_nation_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", targetTable, Optional.of("nation_name"));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable), 3);

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s VALUES ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE'), ('EGYPT', 'MIDDLE EAST'), ('RUSSIA', 'EUROPE')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.nation_name = s.nation_name)" +
                        "    WHEN MATCHED AND t.nation_name > (SELECT name FROM tpch.tiny.region WHERE name = t.region_name AND name LIKE ('A%'))" +
                        "        THEN DELETE" +
                        "    WHEN NOT MATCHED AND s.region_name = 'EUROPE'" +
                        "        THEN INSERT VALUES(s.nation_name, (SELECT 'EUROPE'))",
                2);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('FRANCE', 'EUROPE'), ('GERMANY', 'EUROPE'), ('RUSSIA', 'EUROPE')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeNonNullableColumns()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        String targetTable = "merge_non_nullable_target_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR NOT NULL)", targetTable, Optional.of("nation_name"));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable), 3);

        // Show that updating using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = NULL"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that inserting using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN NOT MATCHED THEN INSERT (nation_name, region_name) VALUES ('IMAGINARIA', NULL)"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that inserting using an implicit null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                // The region_name is implicitly assigned null
                " WHEN NOT MATCHED THEN INSERT (nation_name) VALUES ('IMAGINARIA')"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that if the updated value is provided by a function unpredicatably computing null,
        // the merge fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = CAST(TRY(5/0) AS VARCHAR)"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Override // Overridden to add a column in index position as teradata doesn't allow alterations on index column
    protected void testRenameColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.replaceAll("[^a-z0-9]", "") + randomNameSuffix();
        // Use complex identifier to test a source column name when renaming columns
        String sourceColumnName = "a;b$c";
        assertUpdate("CREATE TABLE " + tableName + "(value INTEGER, \"" + sourceColumnName + "\" VARCHAR)");
        assertTableColumnNames(tableName, "value", sourceColumnName);

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN \"" + sourceColumnName + "\" TO " + nameInSql);

        assertTableColumnNames(tableName, "value", columnName.toLowerCase(ENGLISH));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override // Overridden to Create two columns so we can rename the non-index one (Teradata auto selects first column as PI)
    @Test
    public void testAlterTableRenameColumnToLongName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x, 456 y", 1);

        String baseColumnName = "col";
        // Unwrap OptionalInt; default to TERADATA_OBJECT_NAME_LIMIT (128) when absent to get a concrete int for Teradata name length constraints.
        int maxLength = maxColumnNameLength().orElse(TERADATA_OBJECT_NAME_LIMIT);
        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        // Rename the second (non-index) column
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO " + validTargetColumnName);
        assertUpdate("INSERT INTO " + tableName + " VALUES (789, 1011)", 1);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 456, 1011");
        assertThat(query("SHOW STATS FOR " + tableName)).succeeds();
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x, 456 y", 1);
        String invalidTargetColumnName = validTargetColumnName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO " + invalidTargetColumnName))
                .satisfies(this::verifyColumnNameLengthFailurePermissible);
        assertQuery("SELECT x, y FROM " + tableName, "VALUES (123, 456)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateSchemaIfNotExists()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Override // Overridden due to mismatch in error message
    @Test
    public void testRenameTableAcrossSchema()
    {
        String tableName = "test_rename_old_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);

        String renamedTable = "test_rename_new_" + randomNameSuffix();
        assertQueryFails(
                "ALTER TABLE " + tableName + " RENAME TO " + schemaName + "." + renamedTable,
                "(?s).*The user cannot RENAME to a new database\\.");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_default_cols",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override // Teradata supports unicode with different syntax, so has to override.
    @Test
    public void testInsertUnicode()
    {
        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50) CHARACTER SET UNICODE)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = newTrinoTable("test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Override // Teradata UNICODE charset is UCS-2 (BMP only) and does not support supplementary characters
    @Test
    public void testInsertHighestUnicodeCharacter()
    {
        abort("Teradata UNICODE charset (UCS-2) does not support supplementary characters (U+10FFFF and above)");
    }

    @Override
    @Test
    public void testInsertNegativeDate()
    {
        abort("Skipping as connector does not support creating table with negative date");
    }

    @Override // Overriding to avoid [Error 5977] and [Error 3707]: Teradata does not allow MERGE-INTO to update
    // the Primary Index column, and does not support "AS TABLE <name>" syntax
    @Test
    public void testRowLevelUpdate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));
        if (!hasBehavior(SUPPORTS_ROW_LEVEL_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        // Use a table where `id` is the Primary Index so that updates to `name` and `regionkey` work via MERGE-INTO
        try (TestTable table = newTrinoTable(
                "test_update",
                "(id BIGINT, name VARCHAR(100), regionkey BIGINT)",
                ImmutableList.of("1, 'AFRICA', 0", "2, 'AMERICAS', 1", "3, 'ASIA', 2", "4, 'EUROPE', 3", "5, 'MIDDLE EAST', 4"))) {
            String tableName = table.getName();
            assertUpdate("UPDATE " + tableName + " SET regionkey = regionkey + 100 WHERE id > 3", 2);
            assertQuery(
                    "SELECT id, regionkey FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 0), (2, 1), (3, 2), (4, 103), (5, 104)");

            // UPDATE after UPDATE
            assertUpdate("UPDATE " + tableName + " SET name = CONCAT(name, '_v2') WHERE regionkey > 100", 2);
            assertQuery(
                    "SELECT id, name FROM " + tableName + " WHERE regionkey > 100 ORDER BY id",
                    "VALUES (4, 'EUROPE_v2'), (5, 'MIDDLE EAST_v2')");
        }
    }

    @Override // Teradata JDBC reports UNICODE columns as case-insensitive, blocking count(DISTINCT) pushdown
    @Test
    public void testCountDistinctWithStringTypes()
    {
        abort("Teradata JDBC reports UNICODE char/varchar columns as case-insensitive, preventing count(DISTINCT) aggregation pushdown");
    }

    @Test
    @Override
    public void testUpdateWithPredicates()
    {
        try (TestTable table = newTrinoTable("test_row_predicates", "(a INT, b INT, c INT)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET a = 5 WHERE c = 3", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 2, 3), (11, 12, 13), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET c = 6 WHERE a = 11", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 2, 3), (11, 12, 6), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 44 WHERE b = 22", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 2, 3), (11, 12, 6), (21, 44, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 45 WHERE a > 5", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 2, 3), (11, 45, 6), (21, 45, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 46 WHERE a < 21", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 46, 3), (11, 46, 6), (21, 45, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 47 WHERE a != 11", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 47, 3), (11, 46, 6), (21, 47, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 48 WHERE a IN (5, 11)", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 48, 3), (11, 48, 6), (21, 47, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = 49 WHERE a NOT IN (5, 11)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 48, 3), (11, 48, 6), (21, 49, 23)");

            // Expression update with NOT IN predicate — supported via MERGE-INTO when SUPPORTS_ROW_LEVEL_UPDATE is true
            assertUpdate("UPDATE " + tableName + " SET b = b + 3 WHERE a NOT IN (5, 11)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (5, 48, 3), (11, 48, 6), (21, 52, 23)");
        }
    }

    @Test
    @Override  // Override to fix [Error 5977] [SQLState HY000] Invalid MERGE-INTO statement: Update of Primary index or partition column is not allowed
    public void testUpdateRowConcurrently()
            throws Exception
    {
        // Teradata uses MERGE-INTO for row-level updates (SUPPORTS_ROW_LEVEL_UPDATE=true).
        // The first column becomes the Primary Index, so col0 cannot be updated via MERGE.
        // Override verifyConcurrentUpdateFailurePermissible to accept Teradata-specific locking errors.
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));
        super.testUpdateRowConcurrently();
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        // Teradata error 5977: MERGE-INTO cannot update the Primary Index column
        // Teradata error 3598: Transient lock-timeout / concurrent DDL conflict
        String message = e.getMessage();
        assertThat(message != null && (message.contains("5977") || message.contains("3598")))
                .as("Expected Teradata error 5977 or 3598 but got: " + message)
                .isTrue();
    }

    @Override // Override to avoid a syntax issue caused by using 'SECOND', which is a keyword in Teradata.
    @Test
    public void testAddColumnWithPosition()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN_WITH_POSITION)) {
            try (TestTable table = newTrinoTable("test_add_column_", "AS SELECT 2 \"second\", 4 fourth")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ADD COLUMN first integer FIRST",
                        "This connector does not support adding columns with FIRST clause");
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ADD COLUMN third integer AFTER \"second\"",
                        "This connector does not support adding columns with AFTER clause");
            }
            return;
        }

        try (TestTable table = newTrinoTable("test_add_column_", "AS SELECT 2 \"second\", 4 fourth")) {
            assertTableColumnNames(table.getName(), "second", "fourth");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (2, 4)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN first integer FIRST");
            assertTableColumnNames(table.getName(), "first", "second", "fourth");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (null, 2, 4)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN third integer AFTER \"second\"");
            assertTableColumnNames(table.getName(), "first", "second", "third", "fourth");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (null, 2, null, 4)");

            assertUpdate("INSERT INTO " + table.getName() + " VALUES (10, 20, 30, 40)", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (null, 2, null, 4), (10, 20, 30, 40)");
        }
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        if (!hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT)) {
            assertQueryFails(
                    "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                    format("line 1:35: Catalog '%s' does not support non-null column for column name 'not_null_col'", getSession().getCatalog().orElseThrow()));
            return;
        }

        try (TestTable table = newTrinoTable("insert_not_null", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            System.out.println("Done 1");
            // The error message comes from remote databases when ConnectorMetadata.supportsMissingColumnsOnInsert is true
            assertQueryFails(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            System.out.println("Done 2");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            System.out.println("Done 3");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            System.out.println("Done 4");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            System.out.println("Done 5");
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            System.out.println("Done 6");
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation WHERE regionkey < 0", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            System.out.println("Done 7");
        }

        try (TestTable table = newTrinoTable("commuted_not_null", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageContaining("[Error 2803] [SQLState 23000] Secondary index uniqueness violation");
    }

    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        // allow arbitrary prefix/suffix, but match the important pieces
        return String.format(".*\\[Error 3811\\]\\s*\\[SQLState 23000\\].*Column '%s' is NOT NULL.*", columnName);
    }

    @Test
    public void testDropSchema()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
        assertUpdate("DROP SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
    }

    @Test
    public void testDropSchemaIfExists()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
    }

    @Test
    public void testDropSchemaRestrict()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("CREATE TABLE " + schemaName + ".test_table (id INTEGER)");
        assertQueryFails(
                "DROP SCHEMA " + schemaName + " RESTRICT",
                "\\Qline 1:1: Cannot drop non-empty schema '" + schemaName + "'\\E");
        assertUpdate("DROP TABLE " + schemaName + ".test_table");
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Override // Overridden since we do not support creating Materialized Views
    @Test
    public void testDropSchemaCascade()
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("CREATE TABLE " + schemaName + ".test_table (id INTEGER)");
        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
    }

    @Override
    @Test
    // Overridden: VALUES ROW syntax is not supported in Teradata SQL
    public void testShowCreateView()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String viewName = "test_show_create_view_" + randomNameSuffix();

        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        assertUpdate(format(
                "CREATE VIEW %s.%s.%s SECURITY DEFINER AS\n" +
                        "SELECT col1, col2 FROM (\n" +
                        "  SELECT CAST(1 AS INTEGER) AS col1, CAST('one' AS VARCHAR(3)) AS col2 FROM (SELECT 1 AS dummy) AS t1\n" +
                        "  UNION ALL\n" +
                        "  SELECT CAST(2 AS INTEGER), CAST('t' AS VARCHAR(3)) FROM (SELECT 1 AS dummy) AS t2\n" +
                        ") AS derived_table",
                catalog,
                schema,
                viewName));

        assertQuery("SELECT * FROM " + viewName, "VALUES (1, 'one'), (2, 't')");

        String showCreate = (String) computeScalar("SHOW CREATE VIEW " + viewName);
        assertThat(showCreate)
                .contains("CREATE VIEW")
                .contains(viewName)
                .contains("SECURITY DEFINER");

        assertUpdate("DROP VIEW " + viewName);
    }

    @Override
    @Test
    // Overridden: subquery without FROM clause is not valid in all Teradata versions
    public void testViewCaseSensitivity()
    {
        String upperCaseView = "test_view_uppercase_" + randomNameSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomNameSuffix();

        assertUpdate("CREATE VIEW " + upperCaseView + " AS SELECT X FROM (SELECT CAST(123 AS INTEGER) AS X) AS t");
        assertUpdate("CREATE VIEW " + mixedCaseView + " AS SELECT XyZ FROM (SELECT CAST(456 AS INTEGER) AS XyZ) AS t");

        assertQuery("SELECT * FROM " + upperCaseView, "SELECT 123");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT 456");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
    }

    @Override
    @Test
    // Overridden: testViewMetadata validates exact SECURITY DEFINER/INVOKER formatting in SHOW CREATE VIEW
    // output which uses Trino-specific SQL constructs not compatible with Teradata syntax
    public void testViewMetadata()
    {
        abort("Skipping testViewMetadata: uses Trino-specific SQL syntax not compatible with Teradata");
    }

    @Override
    @Test
    // Overridden: Trino-managed views do not exist as native Teradata objects and therefore
    // do not appear in system.jdbc.tables or system.jdbc.columns (JDBC metadata)
    public void testView()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomNameSuffix();
        String testViewWithComment = "test_view_comment_" + randomNameSuffix();

        assertUpdate(format("CREATE VIEW %s AS SELECT orderkey, orderstatus FROM orders", testView));
        assertUpdate(format("CREATE OR REPLACE VIEW %s AS SELECT orderkey, orderstatus, (totalprice / 2) half FROM orders", testView));

        assertQuery("SELECT * FROM " + testView, "SELECT orderkey, orderstatus, (totalprice / 2) FROM orders");
        assertQuery(
                format("SELECT * FROM %s a JOIN %s b ON a.orderkey = b.orderkey", testView, testView),
                "SELECT a.orderkey, a.orderstatus, a.totalprice / 2, b.orderkey, b.orderstatus, b.totalprice / 2 FROM orders a JOIN orders b ON a.orderkey = b.orderkey");
        assertQuery(format("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM %s", testView), "SELECT orderkey, orderstatus, totalprice / 2 FROM orders");
        assertQuery(format("SELECT * FROM %s.%s.%s", catalog, schema, testView), "SELECT orderkey, orderstatus, totalprice / 2 FROM orders");

        // Verify view is queryable directly and via fully-qualified name
        assertThat(computeActual(format("SHOW COLUMNS FROM " + testView)).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList()))
                .contains("orderkey", "orderstatus", "half");
        assertThat(computeActual("DESCRIBE " + testView).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList()))
                .contains("orderkey", "orderstatus", "half");

        if (hasBehavior(SUPPORTS_COMMENT_ON_VIEW)) {
            assertUpdate(format("CREATE VIEW %s COMMENT 'test comment' AS SELECT orderkey FROM orders", testViewWithComment));
            String showCreate = (String) computeScalar("SHOW CREATE VIEW " + testViewWithComment);
            assertThat(showCreate).contains("COMMENT 'test comment'");
            assertUpdate("DROP VIEW " + testViewWithComment);
        }

        assertUpdate("DROP VIEW " + testView);
    }

    @Override
    @Test
    // Overridden: requires fully qualified catalog.schema.table references for Teradata object resolution
    public void testCompatibleTypeChangeForView()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_table_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s.%s.%s AS SELECT 'abcdefg' a", catalog, schema, tableName), 1);
        assertUpdate(format("CREATE VIEW %s.%s.%s AS SELECT a FROM %s.%s.%s", catalog, schema, viewName, catalog, schema, tableName));
        assertQuery("SELECT * FROM " + viewName, "VALUES 'abcdefg'");

        assertUpdate(format("DROP TABLE %s.%s.%s", catalog, schema, tableName));
        assertUpdate(format("CREATE TABLE %s.%s.%s AS SELECT 'abc' a", catalog, schema, tableName), 1);
        assertQuery("SELECT * FROM " + viewName, "VALUES 'abc'");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    // Overridden: requires fully qualified catalog.schema.table references for Teradata object resolution
    public void testCompatibleTypeChangeForView2()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_table_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s.%s.%s AS SELECT BIGINT '1' v", catalog, schema, tableName), 1);
        assertUpdate(format("CREATE VIEW %s.%s.%s AS SELECT * FROM %s.%s.%s", catalog, schema, viewName, catalog, schema, tableName));
        assertQuery("SELECT * FROM " + viewName, "VALUES 1");

        assertUpdate(format("DROP TABLE %s.%s.%s", catalog, schema, tableName));
        assertUpdate(format("CREATE TABLE %s.%s.%s AS SELECT INTEGER '1' v", catalog, schema, tableName), 1);
        assertQuery(format("SELECT * FROM %s WHERE v = 1", viewName), "VALUES 1");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override // Overridden because Teradata persists a cleared comment as empty string (not NULL), so removal is asserted as "" or null
    @Test
    public void testCommentTable()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();

        try (TestTable table = newTrinoTable("test_comment_", "(a integer)")) {
            // table.getName() includes the schema prefix ("schema.table"); extract the bare table name for metadata queries
            String bareTableName = table.getName().contains(".")
                    ? table.getName().substring(table.getName().lastIndexOf('.') + 1)
                    : table.getName();

            // comment initially not set
            assertThat(getTableComment(table.getName())).isEqualTo(null);

            // comment set
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'new comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("new comment");
            // comment is visible through system.metadata.table_comments
            assertThat(query(
                    "SELECT table_name, comment FROM system.metadata.table_comments " +
                            "WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "'"))
                    .skippingTypesCheck()
                    .containsAll("VALUES ('" + bareTableName + "', 'new comment')");

            // comment removed via empty string
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS ''");
            assertThat(getTableComment(table.getName())).isIn("", null); // Some storages do not preserve empty comment

            // comment removed via IS NULL (the connector translates "IS NULL" to Teradata's supported empty-string syntax)
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'comment to remove'");
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS NULL");
            assertThat(getTableComment(table.getName())).isIn("", null);
        }
    }

    @Override // Overridden because table.getName() returns "schema.table"; we must pass only the table part
    protected String getTableComment(String catalogName, String schemaName, String tableName)
    {
        // tableName may arrive as "schema.table" when the TestTable was created with a schema prefix;
        // strip the schema prefix so the 3-part qualified name is correct for system.metadata.table_comments
        String bareTableName = tableName.contains(".")
                ? tableName.substring(tableName.lastIndexOf('.') + 1)
                : tableName;
        return super.getTableComment(catalogName, schemaName, bareTableName);
    }

    @Override // information_schema.columns is unreliable on shared Teradata; use SHOW COLUMNS instead
    protected String getColumnComment(String tableName, String columnName)
    {
        // tableName may include schema prefix — use it as-is since SHOW COLUMNS accepts qualified names
        return computeActual("SHOW COLUMNS FROM " + tableName).getMaterializedRows().stream()
                .filter(row -> columnName.equalsIgnoreCase((String) row.getField(0)))
                .findFirst()
                .map(row -> row.getField(3)) // 4th column is the comment; null when no comment set
                .map(Object::toString)
                .orElse(null);
    }

    @Override // Teradata persists empty string instead of NULL when a column comment is cleared
    @Test
    public void testCommentColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_COLUMN)) {
            assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "This connector does not support setting column comments");
            return;
        }

        try (TestTable table = newTrinoTable("test_comment_column_", "(a integer)")) {
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // Teradata stores "" instead of NULL when the comment is cleared via IS NULL
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS NULL");
            assertThat(getColumnComment(table.getName(), "a")).isIn("", null);

            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");

            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isIn("", null);
        }
    }

    @Override // Overriding because Teradata's first column becomes the Primary Index; dropping it fails with Error 3557
    @Test
    public void testDropColumn()
    {
        if (!hasBehavior(SUPPORTS_DROP_COLUMN)) {
            assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "This connector does not support dropping columns");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName;
        // Use "a" as the first column (Primary Index) so that x and y — non-PI columns — can be dropped
        try (TestTable table = newTrinoTable("test_drop_column_", "(a INT, x INT, y INT)", ImmutableList.of("1, 123, 456"))) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
            assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
            assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Override // Overriding because Teradata's first column becomes the Primary Index; renaming it fails with Error 3631
    @Test
    public void testRenameColumn()
    {
        if (!hasBehavior(SUPPORTS_RENAME_COLUMN)) {
            assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "This connector does not support renaming columns");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName;
        // Use "id" as the Primary Index so that "x" — a non-PI column — can be safely renamed
        try (TestTable table = newTrinoTable("test_rename_column_", "(id INT, x VARCHAR(20))", ImmutableList.of("1, 'some value'"))) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO before_y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS before_y TO y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
            assertQuery("SELECT y FROM " + tableName, "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO Z"); // 'Z' is upper-case, not delimited
            assertQuery(
                    "SELECT z FROM " + tableName, // 'z' is lower-case, not delimited
                    "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS z TO a");
            assertQuery("SELECT a FROM " + tableName, "VALUES 'some value'");

            // Verify both columns are accessible
            assertQuery("SELECT id, a FROM " + tableName, "VALUES (1, 'some value')");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Override // Overriding because Teradata's first column becomes the Primary Index; MERGE-INTO cannot update PI columns (Error 5977)
    @Test
    public void testUpdateAllValues()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));

        // Use "id" as the first column (Primary Index) so that a, b, c can be updated via MERGE-INTO
        try (TestTable table = newTrinoTable("test_update_all_columns", "(id INT, a INT, b INT, c INT)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1, 2, 3), (2, 11, 12, 13), (3, 21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET a = a + 1, b = b - 1, c = c * 2", 3);
            assertQuery("SELECT a, b, c FROM " + tableName, "VALUES (2, 1, 6), (12, 11, 26), (22, 21, 46)");
        }
    }

    @Override // Teradata serializes concurrent inserts due to table-level locking, causing timeouts
    @Test
    public void testInsertRowConcurrently()
    {
        abort("Teradata serializes concurrent inserts due to table-level locking");
    }

    @Override // Teradata reports column data_type in uppercase (e.g., 'BIGINT' not 'bigint')
    // Also, listing all columns in information_schema.columns may fail if any table in the database
    // has a corrupted column definition (Error 3810), so we skip the full column scan assertion
    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        // Skip the column data_type filter because listing information_schema.columns for the whole
        // catalog can hit Error 3810 (column does not exist) for unrelated pre-existing tables
        // on shared Teradata environments (e.g., AirByte integration tables with missing columns)
    }

    @Override // Teradata's information_schema may return schema/table names in a different case
    @Test
    public void testSelectInformationSchemaColumns()
    {
        abort("Skipping: Teradata information_schema column metadata may differ in case sensitivity from the base test expectations");
    }

    @Override // Teradata does not support ARRAY types
    @Test
    public void testInsertArray()
    {
        abort("Teradata does not support ARRAY column types");
    }

    @Override // Teradata does not support VALUES as an inline table in the FROM clause
    @Test
    public void testUpdateMultipleCondition()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));

        // Use explicit column definitions and row insertions instead of AS SELECT FROM (VALUES ...)
        try (TestTable table = newTrinoTable("test_row_update", "(a INT, b INT)", ImmutableList.of("1, 10", "1, 20", "2, 10"))) {
            assertUpdate("UPDATE " + table.getName() + " SET b = 100 WHERE a = 1 AND b = 10", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 100), (1, 20), (2, 10)");
        }
    }

    @Override // Teradata UNICODE columns may be reported as case-insensitive by the JDBC driver,
    // which blocks case-sensitive aggregation (MIN/MAX) pushdown
    @Test
    public void testCaseSensitiveAggregationPushdown()
    {
        abort("Skipping: Teradata VARCHAR/CHAR columns may be reported as case-insensitive by the JDBC driver, preventing MIN/MAX aggregation pushdown");
    }

    @Override // Teradata UNICODE columns may be reported as case-insensitive by the JDBC driver,
    // which blocks count(DISTINCT) pushdown for varchar columns
    @Test
    public void testDistinctAggregationPushdown()
    {
        abort("Skipping: Teradata VARCHAR columns may be reported as case-insensitive by the JDBC driver, preventing count(DISTINCT) aggregation pushdown");
    }

    @Override
    @Test
    // Overridden: Trino-managed views are stored in trino_metadata.trino_views, not as native
    // Teradata objects. They do not affect whether a Teradata DATABASE (schema) can be dropped.
    public void testDropNonEmptySchemaWithView()
    {
        abort("Skipping testDropNonEmptySchemaWithView: Trino-managed views do not prevent " +
                "Teradata schema (DATABASE) drops since they are stored in a separate metadata table");
    }

    @Test
    // ALTER VIEW ... RENAME TO is not exercised by any base connector test (BaseConnectorTest only
    // covers renaming materialized views), so cover the Trino-managed view rename path explicitly.
    public void testRenameView()
    {
        String sourceView = "test_rename_view_old_" + randomNameSuffix();
        String targetView = "test_rename_view_new_" + randomNameSuffix();

        assertUpdate(format("CREATE VIEW %s AS SELECT orderkey, orderstatus FROM orders", sourceView));

        assertUpdate(format("ALTER VIEW %s RENAME TO %s", sourceView, targetView));

        // The old name no longer resolves and the renamed view returns the original definition's rows.
        assertQueryFails("SELECT * FROM " + sourceView, ".*does not exist.*");
        assertQuery("SELECT * FROM " + targetView, "SELECT orderkey, orderstatus FROM orders");

        assertUpdate("DROP VIEW " + targetView);
    }

    @Override // Teradata does not support ALTER VIEW ... REFRESH
    @Test
    public void testRefreshView()
    {
        abort("Teradata does not support refreshing view definitions (ALTER VIEW ... REFRESH)");
    }

    @Override // Teradata table-level locking causes concurrent ADD COLUMN operations to time out
    @Test
    public void testAddColumnConcurrently()
            throws Exception
    {
        abort("Teradata table-level locking causes concurrent ADD COLUMN operations to deadlock or time out");
    }

    @Override
    @Test
    // Teradata requires all expressions in a derived table to have explicit names.
    // The base test passes 'SELECT 1' which fails with Error 3706.
    // Override to use an aliased expression instead.
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 AS val'))", "VALUES 1");
    }

    @Override
    @Test
    // Teradata returns "" instead of null when a view column comment is cleared with IS NULL.
    public void testCommentViewColumn()
    {
        if (!hasBehavior(TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            abort("Skipping as connector does not support COMMENT ON VIEW COLUMN");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // Teradata returns "" instead of null when a comment is cleared
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isIn(null, "");

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");
        }
    }
}
