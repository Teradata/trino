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
package io.trino.tests.product.teradata;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.ConfiguredFeatures.assertDefaultConnectors;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(TeradataEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Teradata
@TestGroup.ProfileSpecificTests
class TestTeradata
{
    @Test
    void testConfiguredConnectors(TeradataEnvironment env)
    {
        assertDefaultConnectors(env, "teradata");
    }

    @Test
    void testCreateTableAsSelect(TeradataEnvironment env)
            throws Exception
    {
        String schemaName = "teradata.test_" + randomNameSuffix();
        String tableName = schemaName + ".nation_" + randomNameSuffix();

        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schemaName);
            try {
                assertThat(statement.executeUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation"))
                        .isEqualTo(25);
                try {
                    try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                        assertThat(resultSet.next()).isTrue();
                        assertThat(resultSet.getLong(1)).isEqualTo(25);
                    }
                }
                finally {
                    statement.execute("DROP TABLE " + tableName);
                }
            }
            finally {
                statement.execute("DROP SCHEMA " + schemaName);
            }
        }
    }
}
