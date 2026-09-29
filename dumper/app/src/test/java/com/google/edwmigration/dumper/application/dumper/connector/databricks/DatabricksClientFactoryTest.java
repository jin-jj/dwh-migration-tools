/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksClientFactoryTest {

  private static final String CONNECTOR_NAME =
      DatabricksHiveMetastoreMetadataConnector.CONNECTOR_NAME;
  private static final String HOST = "https://adb-123.4.azuredatabricks.net";

  @Test
  public void normalizeWorkspaceHost_keepsOnlySchemeAndAuthority() {
    assertEquals(HOST, DatabricksClientFactory.normalizeWorkspaceHost(HOST));
    assertEquals(HOST, DatabricksClientFactory.normalizeWorkspaceHost(HOST + "/?o=123#folder"));
    assertEquals(
        HOST, DatabricksClientFactory.normalizeWorkspaceHost("  adb-123.4.azuredatabricks.net "));
  }

  @Test
  public void normalizeWorkspaceHost_convertsJdbcUrl() {
    String jdbcUrl =
        "jdbc:databricks://adb-123.4.azuredatabricks.net:443/default;transportMode=http;ssl=1;"
            + "httpPath=/sql/1.0/warehouses/abc123";

    assertEquals(HOST + ":443", DatabricksClientFactory.normalizeWorkspaceHost(jdbcUrl));
  }

  @Test
  public void requireWarehouse_prefersWarehouseFlag() throws IOException {
    ConnectorArguments arguments =
        arguments("--url", HOST + "/sql/1.0/warehouses/from-url", "--warehouse", " from-flag ");

    assertEquals("from-flag", DatabricksClientFactory.requireWarehouse(arguments));
  }

  @Test
  public void requireWarehouse_readsWarehouseAndEndpointPathsFromUrl() throws IOException {
    assertEquals(
        "abc123",
        DatabricksClientFactory.requireWarehouse(
            arguments("--url", HOST + "/sql/1.0/warehouses/abc123")));
    assertEquals(
        "def456",
        DatabricksClientFactory.requireWarehouse(
            arguments("--url", HOST + "/sql/1.0/endpoints/def456")));
  }

  @Test
  public void requireUrl_rejectsMissingUrl() throws IOException {
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> DatabricksClientFactory.requireUrl(arguments("--warehouse", "wh-1")));

    assertTrue(exception.getMessage().contains("--url"));
  }

  private static ConnectorArguments arguments(String... args) throws IOException {
    String[] allArgs = new String[args.length + 2];
    allArgs[0] = "--connector";
    allArgs[1] = CONNECTOR_NAME;
    System.arraycopy(args, 0, allArgs, 2, args.length);
    return new ConnectorArguments(allArgs);
  }
}
