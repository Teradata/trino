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
package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.projection.ProjectFunctionRewriter;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteCaseSensitiveComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.expression.RewriteLikeEscapeWithCaseSensitivity;
import io.trino.plugin.jdbc.expression.RewriteLikeWithCaseSensitivity;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.weakref.jmx.$internal.guava.collect.ImmutableMap;
import org.weakref.jmx.$internal.guava.collect.ImmutableSet;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.teradata.TeradataConstants.DEFAULT_FALLBACK_FRACTION;
import static io.trino.plugin.teradata.TeradataConstants.DEFAULT_VARCHAR_LENGTH;
import static io.trino.plugin.teradata.TeradataConstants.MAX_FALLBACK_NDV;
import static io.trino.plugin.teradata.TeradataConstants.TERADATA_MAX_SUPPORTED_TIMESTAMP_PRECISION;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.weakref.jmx.$internal.guava.base.Preconditions.checkArgument;
import static org.weakref.jmx.$internal.guava.base.Throwables.throwIfInstanceOf;

public class TeradataClient
        extends BaseJdbcClient
{
    private static final PredicatePushdownController TERADATA_STRING_PUSHDOWN = FULL_PUSHDOWN;
    private static final String VIEW_TABLE_NAME = "trino_views";
    private static final String VIEW_COL_SEPARATOR = "\t";
    private static final String VIEW_ROW_SEPARATOR = "\n";
    private static final String VIEW_ROW_SPLIT_PATTERN = "\\r\\n|\\n|\\r"; // matches \n, \r\n, \r only (not Unicode line separators)

    private final Type jsonType;
    private final TeradataConfig.TeradataCaseSensitivity teradataJDBCCaseSensitivity;
    private final boolean statisticsEnabled;
    private final String metadataSchema;
    private final String catalogName;
    private final String viewTableName;
    private ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private ProjectFunctionRewriter<JdbcExpression, ParameterizedExpression> projectFunctionRewriter;

    @Inject
    public TeradataClient(
            BaseJdbcConfig config,
            TeradataConfig teradataConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier,
            CatalogName catalogName)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.teradataJDBCCaseSensitivity = teradataConfig.getTeradataCaseSensitivity();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        this.metadataSchema = teradataConfig.getViewMetadataSchema();
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        this.viewTableName = quoted(null, metadataSchema, VIEW_TABLE_NAME);
        buildExpressionRewriter();
        buildAggregateRewriter();
        buildProjectionFunctionRewriter();
    }

    public static ColumnMapping timeColumnMapping(int precision)
    {
        TimeType timeType = createTimeType(precision);
        return ColumnMapping.longMapping(timeType, timeReadFunction(timeType), timeWriteFunction(precision), DISABLE_PUSHDOWN);
    }

    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        return (resultSet, columnIndex) -> {
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex);
            LocalTime localTime = sqlTimestamp.toLocalDateTime().toLocalTime();
            long nsOfDay = localTime.toNanoOfDay();
            long picosOfDay = nsOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        };
    }

    public static LongWriteFunction timeWriteFunction(int precision)
    {
        return LongWriteFunction.of(Types.TIME, (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromTrinoTime(picosOfDay));
        });
    }

    public static ColumnMapping timeWithTimeZoneColumnMapping(int precision)
    {
        return ColumnMapping.longMapping(createTimeWithTimeZoneType(precision), shortTimeWithTimeZoneReadFunction(), shortTimeWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static LongReadFunction shortTimeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            LocalDateTime localDateTime = sqlTimestamp.toLocalDateTime();
            ZoneId zone = ZoneId.of(calendar.getTimeZone().getID());
            ZonedDateTime zdt = ZonedDateTime.of(localDateTime, zone);
            int offsetMinutes = zdt.getOffset().getTotalSeconds() / 60;
            long nanos = localDateTime.getLong(ChronoField.NANO_OF_DAY);
            return packTimeWithTimeZone(nanos, offsetMinutes);
        };
    }

    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    public static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(createTimestampWithTimeZoneType(precision), shortTimestampWithTimeZoneReadFunction(), shortTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(createTimestampWithTimeZoneType(precision), longTimestampWithTimeZoneReadFunction(), longTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime dateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            OffsetDateTime offsetDateTime = dateTime.toOffsetDateTime();
            long picosOfSecond = offsetDateTime.getNano() * ((long) PICOSECONDS_PER_NANOSECOND);

            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(offsetDateTime.toEpochSecond(), picosOfSecond, getTimeZoneKey(offsetDateTime.toZonedDateTime().getZone().getId()));
        });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMillis = value.getEpochMillis();
            long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
            ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant instant = Instant.ofEpochSecond(epochSeconds);
            statement.setObject(index, OffsetDateTime.ofInstant(instant, zoneId));
        });
    }

    private static ColumnMapping charColumnMapping(int charLength, boolean isCaseSensitive)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength, isCaseSensitive);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, boolean isCaseSensitive)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    private static SliceWriteFunction typedVarcharWriteFunction()
    {
        String bindExpression = format("CAST(? AS %s)", "JSON");

        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                if (value == null) {
                    statement.setNull(index, Types.OTHER);
                    return;
                }
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    private static boolean isMetadataTableMissing(SQLException e)
    {
        // Teradata 3807: Object does not exist; 3802: Database does not exist
        return e.getErrorCode() == 3807 || e.getErrorCode() == 3802;
    }

    private static String serializeColumns(List<ConnectorViewDefinition.ViewColumn> columns)
    {
        StringBuilder sb = new StringBuilder();
        for (ConnectorViewDefinition.ViewColumn col : columns) {
            sb.append(escapeViewField(col.getName()))
                    .append(VIEW_COL_SEPARATOR)
                    .append(escapeViewField(col.getType().getId()));
            // Write the comment field only when a comment is present, so an absent comment (no third field)
            // stays distinct from an intentionally empty comment (present third field, possibly empty).
            if (col.getComment().isPresent()) {
                sb.append(VIEW_COL_SEPARATOR)
                        .append(escapeViewField(col.getComment().get()));
            }
            sb.append(VIEW_ROW_SEPARATOR);
        }
        return sb.toString();
    }

    private static List<ConnectorViewDefinition.ViewColumn> deserializeColumns(String data)
    {
        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> result = ImmutableList.builder();
        for (String line : data.split(VIEW_ROW_SPLIT_PATTERN)) {
            if (line.isEmpty()) {
                continue;
            }
            int firstTab = line.indexOf(VIEW_COL_SEPARATOR);
            if (firstTab < 0) {
                throw new TrinoException(JDBC_ERROR, "Corrupted view column data: " + line);
            }
            String name = unescapeViewField(line.substring(0, firstTab));
            String rest = line.substring(firstTab + 1);
            int secondTab = rest.indexOf(VIEW_COL_SEPARATOR);
            String typeId;
            Optional<String> comment;
            if (secondTab < 0) {
                // Legacy rows hold only name and type; treat a missing third field as no column comment.
                typeId = unescapeViewField(rest);
                comment = Optional.empty();
            }
            else {
                typeId = unescapeViewField(rest.substring(0, secondTab));
                // A present third field is the comment value, including an intentionally empty string.
                comment = Optional.of(unescapeViewField(rest.substring(secondTab + 1)));
            }
            result.add(new ConnectorViewDefinition.ViewColumn(name, TypeId.of(typeId), comment));
        }
        return result.build();
    }

    // Trino permits control characters (including the separators below) in delimited identifiers,
    // so view column names are escaped before storage and reversed on read to keep the round-trip lossless.
    private static String escapeViewField(String value)
    {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescapeViewField(String value)
    {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                char next = value.charAt(++i);
                switch (next) {
                    case '\\' -> sb.append('\\');
                    case 't' -> sb.append('\t');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    default -> sb.append('\\').append(next);
                }
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private boolean deriveCaseSensitivity(CaseSensitivity caseSensitivity)
    {
        return switch (teradataJDBCCaseSensitivity) {
            case CASE_INSENSITIVE -> false;
            case CASE_SENSITIVE -> true;
            default -> caseSensitivity != null;
        };
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> {
            return sql.replaceFirst("(?i)^SELECT", "SELECT TOP " + limit);
        });
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        // Teradata supports TOP N with ORDER BY for all data types
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            // Collect selected columns
            Set<String> selectColumns = new HashSet<>();
            Matcher matcher = Pattern.compile("(?i)SELECT\\s+(.*?)\\s+FROM").matcher(query);
            if (matcher.find()) {
                String[] cols = matcher.group(1).split(",");
                for (String col : cols) {
                    selectColumns.add(col.trim().replaceAll("\"", ""));
                }
            }

            // Add missing ORDER BY columns to SELECT
            List<String> extraColumns = new ArrayList<>();
            for (JdbcSortItem sortItem : sortItems) {
                String columnName = sortItem.column().getColumnName();
                if (!selectColumns.contains(columnName)) {
                    extraColumns.add("\"" + columnName + "\"");
                }
            }

            String modifiedQuery = query;
            if (!extraColumns.isEmpty()) {
                String allColumns = String.join(", ", selectColumns.stream().map(c -> "\"" + c + "\"").toList());
                allColumns += ", " + String.join(", ", extraColumns);
                modifiedQuery = query.replaceFirst("(?i)SELECT\\s+(.*?)\\s+FROM", "SELECT " + allColumns + " FROM");
            }

            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String columnName = quoted(sortItem.column().getColumnName());
                        boolean asc = sortItem.sortOrder().isAscending();
                        String direction = asc ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return columnName + " " + direction + " " + nullsHandling;
                    })
                    .collect(Collectors.joining(", "));

            // Remove schema qualification (e.g. trino.nation → nation)
            String baseQuery = modifiedQuery.replaceAll("\\w+\\.\\w+\\.", "");

            return format("SELECT TOP %d * FROM (%s) AS t ORDER BY %s", limit, baseQuery, orderBy);
        });
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            TeradataStatisticsDao dao = new TeradataStatisticsDao(handle);
            long rowCount = dao.estimateRowCount(table);

            // Fallback to SAMPLE
            if (rowCount <= 0) {
                OptionalLong fallbackCount = dao.sampleRowCountEstimate(table, connection);
                if (fallbackCount.isEmpty()) {
                    return TableStatistics.empty();
                }
                rowCount = fallbackCount.getAsLong();
            }

            Map<String, TeradataStatisticsDao.ColumnIndexStatistics> stats = dao.getColumnIndexStatistics(table);
            TableStatistics.Builder tableStats = TableStatistics.builder().setRowCount(Estimate.of(rowCount));

            for (JdbcColumnHandle column : JdbcMetadata.getColumns(session, this, table)) {
                String columnName = column.getColumnName();
                TeradataStatisticsDao.ColumnIndexStatistics stat = stats.get(columnName);

                ColumnStatistics.Builder columnStats = ColumnStatistics.builder();

                if (stat != null) {
                    columnStats.setNullsFraction(Estimate.of((double) stat.nullCount() / rowCount));

                    long distinctValues = stat.distinctValues();
                    if (distinctValues <= 0) {
                        // No NDV info from Teradata, fallback
                        columnStats.setDistinctValuesCount(Estimate.of(computeFallbackNDV(rowCount)));
                    }
                    else {
                        columnStats.setDistinctValuesCount(Estimate.of(distinctValues));
                    }
                }
                else {
                    // No stats at all for this column, fallback both null fraction and NDV
                    columnStats.setNullsFraction(Estimate.of(0.0));
                    columnStats.setDistinctValuesCount(Estimate.of(computeFallbackNDV(rowCount)));
                }

                tableStats.setColumnStatistics(column, columnStats.build());
            }

            return tableStats.build();
        }
    }

    private long computeFallbackNDV(long rowCount)
    {
        if (rowCount <= 0) {
            return 1; // minimal fallback for empty or invalid row count
        }

        long fallback = (long) (rowCount * DEFAULT_FALLBACK_FRACTION);
        fallback = Math.max(fallback, 1); // at least 1 distinct value
        fallback = Math.min(fallback, MAX_FALLBACK_NDV); // cap at max fallback

        return fallback;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        List<JdbcColumnHandle> columns = getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName);

        String schema = remoteTableName.getSchemaName().orElseThrow();
        String table = remoteTableName.getTableName();

        String query =
                """
                SELECT i.ColumnName
                FROM DBC.IndicesV i
                WHERE i.DatabaseName = '%s'
                  AND i.TableName = '%s'
                  AND i.IndexType IN ('P', 'Q')  -- P = Primary Index, Q = Partitioned PI
                ORDER BY i.ColumnPosition;
                """.formatted(schema, table);

        try (Connection connection = connectionFactory.openConnection(session);
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {
            List<String> primaryIndexNames = new ArrayList<>();
            while (rs.next()) {
                primaryIndexNames.add(rs.getString("COLUMNNAME"));
            }

            if (primaryIndexNames.isEmpty()) {
                return ImmutableList.of();
            }

            return columns.stream()
                    .filter(c -> primaryIndexNames.contains(c.getColumnName()))
                    .collect(toImmutableList());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    public JdbcMergeTableHandle beginMerge(
            ConnectorSession session,
            JdbcTableHandle handle,
            Map<Integer, Collection<ColumnHandle>> updateColumnHandles,
            Consumer<Runnable> rollbackActionCollector,
            RetryMode retryMode)
    {
        if (!supportsMerge()) {
            throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
        }

        // Teradata supports MERGE operation on primary-indexed target table only
        List<JdbcColumnHandle> primaryKeys = getPrimaryKeys(session, handle.getRequiredNamedRelation().getRemoteTableName());
        if (primaryKeys.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The connector can not perform merge on the target table without primary index columns");
        }

        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();

        List<JdbcColumnHandle> columns = getColumns(session, schemaTableName, remoteTableName);

        JdbcTableHandle plainTable = new JdbcTableHandle(schemaTableName, remoteTableName, Optional.empty());

        JdbcOutputTableHandle outputTableHandle = beginInsertTable(session, plainTable, columns);
        rollbackActionCollector.accept(() -> rollbackTemporaryTableCreation(session, outputTableHandle));

        try {
            return new JdbcMergeTableHandle(
                    handle,
                    outputTableHandle,
                    beginUpdate(session, plainTable, columns, primaryKeys, updateColumnHandles, rollbackActionCollector),
                    beginDelete(session, plainTable, primaryKeys, rollbackActionCollector),
                    primaryKeys,
                    columns,
                    updateColumnHandles);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishMerge(ConnectorSession session, JdbcMergeTableHandle handle, Set<Long> pageSinkIds)
    {
        Closer closer = Closer.create();
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            RemoteTableName pageSinkIdsTable = constructPageSinkIdsTable(session, connection, handle.getOutputTableHandle(), pageSinkIds, closer);

            doFinishMerge(session, connection, handle, pageSinkIdsTable, closer);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        finally {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
    }

    private void doFinishMerge(
            ConnectorSession session,
            Connection connection,
            JdbcMergeTableHandle handle,
            RemoteTableName pageSinkTable,
            Closer closer)
            throws SQLException
    {
        try {
            connection.setAutoCommit(false);

            prepareExecuteInsert(session, connection, handle.getOutputTableHandle(), pageSinkTable, closer);
            handle.getUpdateOutputTableHandle().values()
                    .forEach(tableHandle -> prepareExecuteUpdate(session, connection, tableHandle, handle.getPrimaryKeys(), pageSinkTable, closer));
            handle.getDeleteOutputTableHandle().ifPresent(deleteHandle -> prepareExecuteDelete(session, connection, deleteHandle, pageSinkTable, closer));

            connection.commit();
        }
        catch (Throwable e) {
            try {
                connection.rollback();
            }
            catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
            throw new TrinoException(JDBC_ERROR, e);
        }
        finally {
            try {
                connection.setAutoCommit(true);
            }
            catch (SQLException ignore) {
                // best-effort restore
            }
        }
    }

    private void prepareExecuteInsert(
            ConnectorSession session,
            Connection connection,
            JdbcOutputTableHandle handle,
            RemoteTableName pageSinkTable,
            Closer closer)
            throws SQLException
    {
        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());

        String pageSinkIdName = handle.getPageSinkIdColumnName().orElseThrow();

        // ensure temp table is dropped when closer is closed
        closer.register(() -> dropTable(session, temporaryTable, true));

        String columns = handle.getColumnNames().stream()
                .map(this::quoted)
                .collect(joining(", "));

        String insertSql =
                """
                INSERT INTO %s (%s)
                SELECT %s FROM %s temp_table
                WHERE EXISTS (SELECT 1 FROM %s page_sink_table WHERE page_sink_table.%s = temp_table.%s)
                """
                        .formatted(
                                quoted(handle.getRemoteTableName()),
                                columns,
                                columns,
                                quoted(temporaryTable),
                                quoted(pageSinkTable),
                                pageSinkIdName,
                                pageSinkIdName);

        execute(session, connection, insertSql);
    }

    private void prepareExecuteUpdate(
            ConnectorSession session,
            Connection connection,
            JdbcOutputTableHandle handle,
            List<JdbcColumnHandle> primaryKeys,
            RemoteTableName pageSinkTable,
            Closer closer)
    {
        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());

        // ensure temp table is dropped when closer is closed
        closer.register(() -> dropTable(session, temporaryTable, true));

        String targetTableName = quoted(handle.getRemoteTableName());
        String sourceTableName = quoted(temporaryTable);
        String pageSinkTableName = quoted(pageSinkTable);
        String pageSinkIdName = handle.getPageSinkIdColumnName().orElseThrow();

        int keyNamesSize = primaryKeys.size();
        int columnNamesSize = handle.getColumnNames().size();
        checkArgument(columnNamesSize > keyNamesSize, "Update assigns columnNamesSize should be greater than primary key keyNamesSize");

        List<String> updateColumns = handle.getColumnNames().subList(0, columnNamesSize - keyNamesSize);
        List<String> temporaryTableConditionColumns = handle.getColumnNames().subList(columnNamesSize - keyNamesSize, columnNamesSize);

        String updateAssigns = updateColumns.stream()
                .map(col -> quoted(col) + " = src." + quotedUnwrapped(col))
                .collect(joining(", "));

        // build USING select list: keys first, then update columns
        List<String> selectCols = new ArrayList<>(temporaryTableConditionColumns.size() + updateColumns.size());
        for (String col : temporaryTableConditionColumns) {
            selectCols.add("temp_table." + col);
        }
        for (String col : updateColumns) {
            selectCols.add("temp_table." + col);
        }
        String usingSelectList = selectCols.stream().collect(joining(", "));

        ImmutableList.Builder<String> onConditions = ImmutableList.builder();
        for (int i = 0; i < keyNamesSize; i++) {
            String targetPk = primaryKeys.get(i).getColumnName();
            String srcPk = temporaryTableConditionColumns.get(i);
            onConditions.add("tgt." + quotedUnwrapped(targetPk) + " = src." + quotedUnwrapped(srcPk));
        }
        String onClause = String.join(" AND ", onConditions.build());

        String mergeSql =
                """
                MERGE INTO %s AS tgt
                USING (
                  SELECT %s
                  FROM %s AS temp_table
                  JOIN %s AS page_sink_table
                    ON page_sink_table.%s = temp_table.%s
                ) AS src
                ON %s
                WHEN MATCHED THEN
                  UPDATE SET %s
                """.formatted(
                        targetTableName,
                        usingSelectList,
                        sourceTableName,
                        pageSinkTableName,
                        pageSinkIdName,
                        pageSinkIdName,
                        onClause,
                        updateAssigns);

        try {
            execute(session, connection, mergeSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void prepareExecuteDelete(
            ConnectorSession session,
            Connection connection,
            JdbcOutputTableHandle handle,
            RemoteTableName pageSinkTable,
            Closer closer)
    {
        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());

        closer.register(() -> dropTable(session, temporaryTable, true));

        String targetTableName = quoted(handle.getRemoteTableName());
        String sourceTableName = quoted(temporaryTable);
        String pageSinkTableName = quoted(pageSinkTable);
        String pageSinkIdName = handle.getPageSinkIdColumnName().orElseThrow();

        String matchConditions = handle.getColumnNames().stream()
                .map(this::quoted)
                .map(col -> "%s.%s = temp_table.%s".formatted(targetTableName, col, col))
                .collect(Collectors.joining(" AND "));

        String deleteSql =
                """
                DELETE FROM %s
                WHERE EXISTS (
                    SELECT 1
                    FROM %s AS temp_table
                    JOIN %s AS page_sink_table
                      ON page_sink_table.%s = temp_table.%s
                    WHERE %s
                )
                """.formatted(
                        targetTableName,
                        sourceTableName,
                        pageSinkTableName,
                        quoted(pageSinkIdName),
                        quotedUnwrapped(pageSinkIdName),
                        matchConditions);

        try {
            execute(session, connection, deleteSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private String quotedUnwrapped(String column)
    {
        if (column == null || column.length() < 2) {
            return column;
        }
        if (column.startsWith("\"") && column.endsWith("\"")) {
            return column.substring(1, column.length() - 1);
        }
        return column;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
    {
        execute(session, format(
                "CREATE DATABASE %s AS PERMANENT = 60000000, SPOOL = 120000000",
                quoted(remoteSchemaName)));
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s AS ( SELECT * FROM %s ) WITH DATA";
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        int schemaNameLimit = databaseMetadata.getMaxSchemaNameLength();
        if (schemaName.length() > schemaNameLimit) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", schemaNameLimit, schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'", databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            String dropObjects = "DELETE DATABASE " + quoted(remoteSchemaName) + " ALL";
            try {
                execute(session, connection, dropObjects);
            }
            catch (SQLException e) {
                throw new TrinoException(
                        JDBC_ERROR,
                        format("Failed to delete all objects in schema '%s'. You may not have sufficient permissions or the operation may have been blocked. Original error: %s", remoteSchemaName, e.getMessage()),
                        e);
            }
        }
        // Error 3598 is a transient concurrent DDL conflict; retry up to 3 times with back-off
        String dropSchema = "DROP DATABASE " + quoted(remoteSchemaName);
        SQLException lastException = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                execute(session, connection, dropSchema);
                return;
            }
            catch (SQLException e) {
                if (e.getErrorCode() != 3598 || attempt == 3) {
                    throw e;
                }
                lastException = e;
                try {
                    Thread.sleep(500L * attempt);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying DROP DATABASE", ie);
                }
            }
        }
        throw lastException;
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schema");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit(), "Connection must be in auto-commit mode when dropping a column");
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s DROP %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s RENAME %s TO %s",
                quoted(remoteTableName),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName)));
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        execute(session, connection, format(
                "RENAME TABLE %s TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override // Teradata does not support TRUNCATE TABLE; use DELETE <table> ALL instead
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        execute(session, "DELETE " + quoted(handle.asPlainTable().getRemoteTableName()) + " ALL");
    }

    @Override // Overridden to implement table comment support using Teradata's "COMMENT ON TABLE" syntax
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    // Builds the Teradata "COMMENT ON TABLE ... IS ..." statement; an empty comment maps to '' to clear it,
    // because Teradata does not support "IS NULL" (matches the empty-string clearing used by Oracle/Snowflake)
    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("''"));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            // Map the Trino column name to the remote name so identifier mapping is honored, mirroring dropColumn
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            // An empty comment maps to '' to clear it, because Teradata does not support "IS NULL" (matches Oracle)
            String sql = format(
                    "COMMENT ON COLUMN %s.%s IS %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName),
                    comment.map(BaseJdbcClient::varcharLiteral).orElse("''"));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private void buildProjectionFunctionRewriter()
    {
        this.projectFunctionRewriter = new ProjectFunctionRewriter<>(
                connectorExpressionRewriter,
                com.google.common.collect.ImmutableSet.<ProjectFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new RewriteLowerFunction())
                        .add(new RewriteUpperFunction())
                        .add(new RewriteCast((session, type) -> toWriteMapping(session, type).getDataType()))
                        .build());
    }

    private void buildExpressionRewriter()
    {
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .add(new RewriteLikeWithCaseSensitivity())
                .add(new RewriteLikeEscapeWithCaseSensitivity())
                .add(new RewriteSubstring())
                .add(new RewriteLower())
                .add(new RewriteUpper())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .withTypeClass("string_type", ImmutableSet.of("char", "varchar"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$equal(left: string_type, right: string_type)").to("left = right")
                .map("$not_equal(left: string_type, right: string_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .add(new RewriteCaseSensitiveComparison(ImmutableSet.of(ComparisonOperator.EQUAL, ComparisonOperator.NOT_EQUAL)))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulo(left: integer_type, right: integer_type)").to("MOD(left, right)")
                .map("$negate(value: integer_type)").to("-value")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();
    }

    private void buildAggregateRewriter()
    {
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        // Basic aggregate
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(TeradataClient::toTypeHandle))

                        // AVG
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())

                        // Statistical aggregates (numeric types only)
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())

                        // Correlation and regression
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return this.connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> convertProjection(
            ConnectorSession session,
            JdbcTableHandle handle,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        // Reuse the same connector expression rewriter used for predicates
        return projectFunctionRewriter.rewrite(session, handle, expression, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        // try to use result set metadata from select * from table to populate the mapping
        try {
            HashMap<String, CaseSensitivity> caseMap = new HashMap<>();
            String sql = format("select * from %s.%s where 0=1", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSetMetaData rsmd = pstmt.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                caseMap.put(rsmd.getColumnName(i), rsmd.isCaseSensitive(i) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            pstmt.close();
            return caseMap;
        }
        catch (SQLException e) {
            // behavior of base jdbc
            return ImmutableMap.of();
        }
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // this method should ultimately encompass all the expected teradata data types
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        // switch by names as some types overlap other types going by jdbc type alone
        String jdbcTypeName = typeHandle.jdbcTypeName().orElse("VARCHAR");
        Optional<ColumnMapping> nameMapping = switch (jdbcTypeName.toUpperCase(ENGLISH)) {
            case "TIMESTAMP WITH TIME ZONE" -> Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "TIME WITH TIME ZONE" -> Optional.of(timeWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "JSON" -> Optional.of(jsonColumnMapping());
            case "NUMBER" -> numberMapping(typeHandle);
            case "CHARACTER" -> Optional.of(charColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity().orElse(null))));
            case "ARRAY" -> Optional.of(arrayColumnMapping());
            default -> Optional.empty();
        };
        if (nameMapping.isPresent()) {
            return nameMapping;
        }

        Optional<ColumnMapping> typeMapping = switch (typeHandle.jdbcType()) {
            case Types.TINYINT -> Optional.of(tinyintColumnMapping());
            case Types.SMALLINT -> Optional.of(smallintColumnMapping());
            case Types.INTEGER -> Optional.of(integerColumnMapping());
            case Types.BIGINT -> Optional.of(bigintColumnMapping());
            // teradata float is 64 bit
            // trino double is 64 bit
            // teradata float / real / double precision all map to jdbc type float
            case Types.REAL, Types.DOUBLE, Types.FLOAT -> Optional.of(doubleColumnMapping());
            case Types.NUMERIC, Types.DECIMAL -> numberMapping(typeHandle);
            case Types.CHAR -> Optional.of(charColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity().orElse(null))));
            // see prior note on trino case sensitivity
            case Types.VARCHAR -> Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity().orElse(null))));
            case Types.CLOB -> Optional.of(ColumnMapping.sliceMapping(
                    createUnboundedVarcharType(),
                    (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                    varcharWriteFunction(),
                    DISABLE_PUSHDOWN));
            // trino only has varbinary
            case Types.BINARY, Types.VARBINARY, Types.BLOB -> Optional.of(varbinaryColumnMapping());
            case Types.DATE -> Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME -> Optional.of(timeColumnMapping(typeHandle.requiredDecimalDigits()));
            case Types.TIMESTAMP -> Optional.of(timestampColumnMapping(TimestampType.createTimestampType(typeHandle.requiredDecimalDigits())));
            default -> Optional.empty();
        };
        if (typeMapping.isPresent()) {
            return typeMapping;
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private Optional<ColumnMapping> numberMapping(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.requiredColumnSize();
        int scale = typeHandle.requiredDecimalDigits();
        if (precision > Decimals.MAX_PRECISION) {
            // this will trigger for number(*) as precision is 40
            return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale)));
        }
        return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return switch (type) {
            case Type typeInstance when typeInstance.equals(jsonType) -> WriteMapping.sliceMapping("JSON", typedVarcharWriteFunction());
            case Type typeInstance when typeInstance == TINYINT -> WriteMapping.longMapping("smallint", tinyintWriteFunction());
            case Type typeInstance when typeInstance == SMALLINT -> WriteMapping.longMapping("smallint", smallintWriteFunction());
            case Type typeInstance when typeInstance == INTEGER -> WriteMapping.longMapping("integer", integerWriteFunction());
            case Type typeInstance when typeInstance == BIGINT -> WriteMapping.longMapping("bigint", bigintWriteFunction());
            case Type typeInstance when typeInstance == REAL -> WriteMapping.longMapping("FLOAT", realWriteFunction());
            case Type typeInstance when typeInstance == DOUBLE -> WriteMapping.doubleMapping("double precision", doubleWriteFunction());
            case Type typeInstance when VARBINARY.equals(typeInstance) -> WriteMapping.sliceMapping("blob", varbinaryWriteFunction());
            case Type typeInstance when typeInstance == DATE -> WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
            case DecimalType decimalTypeInstance -> {
                String dataType = String.format("decimal(%s, %s)", decimalTypeInstance.getPrecision(), decimalTypeInstance.getScale());
                if (decimalTypeInstance.isShort()) {
                    yield WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalTypeInstance));
                }
                yield WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalTypeInstance));
            }
            case CharType charTypeInstance -> WriteMapping.sliceMapping("char(" + charTypeInstance.getLength() + ")", charWriteFunction());
            case VarcharType varcharTypeInstance -> {
                String dataType = varcharTypeInstance.isUnbounded()
                        ? "varchar(" + DEFAULT_VARCHAR_LENGTH + ")"
                        : "varchar(" + varcharTypeInstance.getBoundedLength() + ")";
                yield WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
            case TimeType timeTypeInstance -> {
                verify(timeTypeInstance.getPrecision() <= TERADATA_MAX_SUPPORTED_TIMESTAMP_PRECISION);
                yield WriteMapping.longMapping(
                        String.format("time(%s)", timeTypeInstance.getPrecision()),
                        timeWriteFunction(timeTypeInstance.getPrecision()));
            }
            case TimestampType timestampTypeInstance -> {
                verify(timestampTypeInstance.getPrecision() <= TERADATA_MAX_SUPPORTED_TIMESTAMP_PRECISION);
                yield WriteMapping.longMapping(
                        String.format("timestamp(%s)", timestampTypeInstance.getPrecision()),
                        timestampWriteFunction(timestampTypeInstance));
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        };
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                jsonReadFunction(),
                typedVarcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private SliceReadFunction jsonReadFunction()
    {
        return (resultSet, columnIndex) -> {
            String json = resultSet.getString(columnIndex);
            if (json == null) {
                return null;
            }
            return jsonParse(utf8Slice(json));
        };
    }

    private ColumnMapping arrayColumnMapping()
    {
        // Default to VARCHAR element type - you can enhance this to detect actual element type
        Type elementType = createUnboundedVarcharType();
        Type arrayType = new ArrayType(elementType);

        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(elementType),
                arrayWriteFunction(elementType),
                DISABLE_PUSHDOWN);
    }

    private ObjectReadFunction arrayReadFunction(Type elementType)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array sqlArray = resultSet.getArray(columnIndex);
            if (sqlArray == null) {
                return null;
            }

            Object[] elements = (Object[]) sqlArray.getArray();
            BlockBuilder blockBuilder = elementType.createBlockBuilder(null, elements.length);

            for (Object element : elements) {
                if (element == null) {
                    blockBuilder.appendNull();
                }
                else {
                    elementType.writeSlice(blockBuilder, utf8Slice(element.toString()));
                }
            }

            return blockBuilder.build();
        });
    }

    private ObjectWriteFunction arrayWriteFunction(Type elementType)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            if (block == null) {
                statement.setNull(index, Types.ARRAY);
                return;
            }

            Object[] elements = new Object[block.getPositionCount()];
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    elements[i] = null;
                }
                else {
                    elements[i] = elementType.getSlice(block, i).toStringUtf8();
                }
            }

            Array sqlArray = statement.getConnection().createArrayOf("VARCHAR", elements);
            statement.setArray(index, sqlArray);
        });
    }

    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        try (Connection conn = connectionFactory.openConnection(session)) {
            verify(conn.getAutoCommit());
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(getRemoteIdentifiers(conn), session.getIdentity(), viewName.getSchemaName());
            verifySchemaExists(conn, remoteSchema);
            ensureMetadataTableExists(session);
            conn.setAutoCommit(false);
            try {
                if (replace) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "DELETE FROM " + viewTableName + " WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
                        stmt.setString(1, catalogName);
                        stmt.setString(2, viewName.getSchemaName());
                        stmt.setString(3, viewName.getTableName());
                        stmt.executeUpdate();
                    }
                }
                try (PreparedStatement stmt = conn.prepareStatement(
                        "INSERT INTO " + viewTableName +
                                " (catalog_name, schema_name, view_name, original_sql, view_owner, run_as_invoker, columns_data, view_comment)" +
                                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    stmt.setString(1, catalogName);
                    stmt.setString(2, viewName.getSchemaName());
                    stmt.setString(3, viewName.getTableName());
                    stmt.setString(4, definition.getOriginalSql());
                    stmt.setString(5, definition.getOwner().orElse(null));
                    stmt.setString(6, definition.isRunAsInvoker() ? "Y" : "N");
                    stmt.setString(7, serializeColumns(definition.getColumns()));
                    stmt.setString(8, definition.getComment().orElse(null));
                    stmt.executeUpdate();
                }
                conn.commit();
            }
            catch (SQLException e) {
                try {
                    conn.rollback();
                }
                catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
            finally {
                try {
                    conn.setAutoCommit(true);
                }
                catch (SQLException ignore) {
                    // best-effort restore
                }
            }
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 2801) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + viewName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to create view: " + viewName, e);
        }
    }

    private boolean schemaExists(Connection conn, String schemaName)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM DBC.DatabasesV WHERE UPPER(DatabaseName) = ?")) {
            stmt.setString(1, schemaName.toUpperCase(ENGLISH));
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private void createMetadataSchema(Connection conn)
            throws SQLException
    {
        try (Statement stmt = conn.createStatement()) {
            // The internal metadata database only holds Trino view definitions, so a modest fixed footprint is allocated:
            // PERMANENT ~60 MB for stored definitions and SPOOL ~120 MB for query workspace.
            stmt.execute(format("CREATE DATABASE %s AS PERMANENT = 60000000, SPOOL = 120000000", quoted(metadataSchema)));
        }
        catch (SQLException e) {
            // Another session may have created the schema concurrently; re-check once and ignore if it now exists.
            if (!schemaExists(conn, metadataSchema)) {
                throw e;
            }
        }
    }

    private void verifySchemaExists(Connection conn, String schemaName)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM DBC.DatabasesV WHERE UPPER(DatabaseName) = ?")) {
            stmt.setString(1, schemaName.toUpperCase(ENGLISH));
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next() || rs.getInt(1) == 0) {
                    throw new SchemaNotFoundException(schemaName);
                }
            }
        }
    }

    private void ensureMetadataTableExists(ConnectorSession session)
    {
        try (Connection conn = connectionFactory.openConnection(session)) {
            if (!schemaExists(conn, metadataSchema)) {
                createMetadataSchema(conn);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to initialize view metadata schema: " + metadataSchema, e);
        }

        if (!metadataTableExists(session)) {
            try {
                execute(session, format(
                        "CREATE TABLE %s (" +
                                "  catalog_name   VARCHAR(128) NOT NULL," +
                                "  schema_name    VARCHAR(128) NOT NULL," +
                                "  view_name      VARCHAR(128) NOT NULL," +
                                "  original_sql   CLOB         NOT NULL," +
                                "  view_owner     VARCHAR(256)," +
                                "  run_as_invoker CHAR(1)      NOT NULL," +
                                "  columns_data   CLOB         NOT NULL," +
                                "  view_comment   CLOB," +
                                "  PRIMARY KEY (catalog_name, schema_name, view_name)" +
                                ")",
                        viewTableName));
            }
            catch (TrinoException e) {
                // Teradata 5612: object already exists — concurrent createView race, safe to ignore
                if (!(e.getCause() instanceof SQLException cause) || cause.getErrorCode() != 5612) {
                    throw new TrinoException(JDBC_ERROR, "Failed to create view metadata table: " + viewTableName, e);
                }
            }
        }
    }

    private boolean metadataTableExists(ConnectorSession session)
    {
        try (Connection conn = connectionFactory.openConnection(session);
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT COUNT(*) FROM DBC.TablesV WHERE UPPER(DatabaseName) = ? AND UPPER(TableName) = ?")) {
            stmt.setString(1, metadataSchema.toUpperCase(ENGLISH));
            stmt.setString(2, VIEW_TABLE_NAME.toUpperCase(ENGLISH));
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to check existence of view metadata table: " + viewTableName, e);
        }
    }

    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection conn = connectionFactory.openConnection(session);
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT original_sql, view_owner, run_as_invoker, columns_data, view_comment FROM " +
                                viewTableName + " WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
            stmt.setString(1, catalogName);
            stmt.setString(2, viewName.getSchemaName());
            stmt.setString(3, viewName.getTableName());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                boolean runAsInvoker = "Y".equals(rs.getString("run_as_invoker"));
                List<ConnectorViewDefinition.ViewColumn> columns = deserializeColumns(rs.getString("columns_data"));
                return Optional.of(new ConnectorViewDefinition(
                        rs.getString("original_sql"),
                        Optional.of(catalogName),
                        Optional.of(viewName.getSchemaName()),
                        columns,
                        Optional.ofNullable(rs.getString("view_comment")),
                        Optional.ofNullable(rs.getString("view_owner")),
                        runAsInvoker,
                        ImmutableList.of()));
            }
        }
        catch (SQLException e) {
            if (isMetadataTableMissing(e)) {
                return Optional.empty();
            }
            throw new TrinoException(JDBC_ERROR, "Failed to get view: " + viewName, e);
        }
    }

    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        try (Connection conn = connectionFactory.openConnection(session);
                PreparedStatement stmt = conn.prepareStatement(
                        "DELETE FROM " + viewTableName + " WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
            verify(conn.getAutoCommit(), "Connection must be in auto-commit mode when dropping a view");
            stmt.setString(1, catalogName);
            stmt.setString(2, viewName.getSchemaName());
            stmt.setString(3, viewName.getTableName());
            if (stmt.executeUpdate() == 0) {
                throw new ViewNotFoundException(viewName);
            }
        }
        catch (SQLException e) {
            if (isMetadataTableMissing(e)) {
                throw new ViewNotFoundException(viewName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to drop view: " + viewName, e);
        }
    }

    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        try (Connection conn = connectionFactory.openConnection(session)) {
            verify(conn.getAutoCommit(), "Connection must be in auto-commit mode when renaming a view");
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(getRemoteIdentifiers(conn), session.getIdentity(), target.getSchemaName());
            verifySchemaExists(conn, remoteSchema);
            try (PreparedStatement stmt = conn.prepareStatement(
                    "UPDATE " + viewTableName +
                            " SET schema_name = ?, view_name = ?" +
                            " WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
                stmt.setString(1, target.getSchemaName());
                stmt.setString(2, target.getTableName());
                stmt.setString(3, catalogName);
                stmt.setString(4, source.getSchemaName());
                stmt.setString(5, source.getTableName());
                if (stmt.executeUpdate() == 0) {
                    throw new ViewNotFoundException(source);
                }
            }
        }
        catch (SQLException e) {
            if (isMetadataTableMissing(e)) {
                throw new ViewNotFoundException(source);
            }
            if (e.getErrorCode() == 2801) {
                throw new TrinoException(ALREADY_EXISTS, "View already exists: " + target);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to rename view " + source + " to " + target, e);
        }
    }

    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        try (Connection conn = connectionFactory.openConnection(session);
                PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE " + viewTableName +
                                " SET view_comment = ? WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
            verify(conn.getAutoCommit(), "Connection must be in auto-commit mode when setting a view comment");
            stmt.setString(1, comment.orElse(null));
            stmt.setString(2, catalogName);
            stmt.setString(3, viewName.getSchemaName());
            stmt.setString(4, viewName.getTableName());
            if (stmt.executeUpdate() == 0) {
                throw new ViewNotFoundException(viewName);
            }
        }
        catch (SQLException e) {
            if (isMetadataTableMissing(e)) {
                throw new ViewNotFoundException(viewName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to set view comment: " + viewName, e);
        }
    }

    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        try (Connection conn = connectionFactory.openConnection(session)) {
            verify(conn.getAutoCommit());
            conn.setAutoCommit(false);
            try {
                List<ConnectorViewDefinition.ViewColumn> columns;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT columns_data FROM " + viewTableName +
                                " WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
                    stmt.setString(1, catalogName);
                    stmt.setString(2, viewName.getSchemaName());
                    stmt.setString(3, viewName.getTableName());
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new ViewNotFoundException(viewName);
                        }
                        columns = deserializeColumns(rs.getString("columns_data"));
                    }
                }
                boolean found = false;
                ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> updated = ImmutableList.builder();
                for (ConnectorViewDefinition.ViewColumn column : columns) {
                    if (column.getName().equals(columnName)) {
                        updated.add(new ConnectorViewDefinition.ViewColumn(column.getName(), column.getType(), comment));
                        found = true;
                    }
                    else {
                        updated.add(column);
                    }
                }
                if (!found) {
                    throw new ColumnNotFoundException(viewName, columnName);
                }
                try (PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE " + viewTableName +
                                " SET columns_data = ? WHERE catalog_name = ? AND schema_name = ? AND view_name = ?")) {
                    stmt.setString(1, serializeColumns(updated.build()));
                    stmt.setString(2, catalogName);
                    stmt.setString(3, viewName.getSchemaName());
                    stmt.setString(4, viewName.getTableName());
                    if (stmt.executeUpdate() == 0) {
                        throw new ViewNotFoundException(viewName);
                    }
                }
                conn.commit();
            }
            catch (SQLException e) {
                try {
                    conn.rollback();
                }
                catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
            finally {
                try {
                    conn.setAutoCommit(true);
                }
                catch (SQLException ignore) {
                    // best-effort restore
                }
            }
        }
        catch (SQLException e) {
            if (isMetadataTableMissing(e)) {
                throw new ViewNotFoundException(viewName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to set view column comment: " + viewName + " column " + columnName, e);
        }
    }

    private record TeradataStatisticsDao(Handle handle)
    {
        private TeradataStatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        public long estimateRowCount(JdbcTableHandle table)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            return handle.createQuery(
                            "SELECT MAX(RowCount) AS est_row_count " +
                                    "FROM DBC.StatsV " +
                                    "WHERE DatabaseName = :schema AND TableName = :table")
                    .bind("schema", schema)
                    .bind("table", tableName)
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(0L);
        }

        public Map<String, ColumnIndexStatistics> getColumnIndexStatistics(JdbcTableHandle table)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            return handle.createQuery(
                            "SELECT ColumnName, NullCount, UniqueValueCount " +
                                    "FROM DBC.StatsV " +
                                    "WHERE DatabaseName = :schema AND TableName = :table")
                    .bind("schema", schema)
                    .bind("table", tableName)
                    .map((rs, _) -> {
                        String column = rs.getString("ColumnName");
                        if (column == null) {
                            // skip this row by returning null
                            return null;
                        }
                        long nullCount = rs.getLong("NullCount");
                        long distinct = rs.getLong("UniqueValueCount");

                        return new SimpleEntry<>(
                                column.trim(),
                                new ColumnIndexStatistics(nullCount > 0, distinct, nullCount));
                    })
                    // Filter out nulls before collecting to map
                    .filter(Objects::nonNull)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public OptionalLong sampleRowCountEstimate(JdbcTableHandle table, Connection connection)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            String sql = format("SELECT COUNT(*) * 100 AS estimated_count FROM %s.%s SAMPLE 1", schema, tableName);

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    long estimated = rs.getLong("estimated_count");
                    return OptionalLong.of(estimated);
                }
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, "Sampling fallback failed: " + e);
            }

            return OptionalLong.empty();
        }

        public record ColumnIndexStatistics(boolean nullable, long distinctValues, long nullCount) {}
    }
}
