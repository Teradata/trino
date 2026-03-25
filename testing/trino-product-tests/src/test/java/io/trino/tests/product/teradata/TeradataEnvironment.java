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

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

/**
 * Product test environment for Teradata connector validation.
 *
 * Wires credentials of an externally provisioned Teradata system into a single
 * Trino container with tpch and teradata catalogs.
 */
public class TeradataEnvironment
        extends ProductTestEnvironment
{
    private Network network;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        String teradataHostname = requireEnv("TERADATA_HOSTNAME");
        String teradataUsername = requireEnv("TERADATA_USERNAME");
        String teradataPassword = requireEnv("TERADATA_PASSWORD");

        network = Network.newNetwork();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("teradata", Map.of(
                        "connector.name", "teradata",
                        "connection-url", "jdbc:teradata://${ENV:TERADATA_HOSTNAME}/",
                        "connection-user", "${ENV:TERADATA_USERNAME}",
                        "connection-password", "${ENV:TERADATA_PASSWORD}"))
                .build();

        trino.withEnv("TERADATA_HOSTNAME", teradataHostname);
        trino.withEnv("TERADATA_USERNAME", teradataUsername);
        trino.withEnv("TERADATA_PASSWORD", teradataPassword);
        TrinoProductTestContainer.startAndWait(trino);
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
