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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.databricks.sdk.WorkspaceClient;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksSqlHelperTest {

  @Test
  public void escapeIdentifier_simpleName_returnsEscaped() {
    assertEquals("`my_schema`", DatabricksSqlHelper.escapeIdentifier("my_schema"));
  }

  @Test
  public void escapeIdentifier_withBackticks_escapesBackticks() {
    assertEquals("`foo``bar`", DatabricksSqlHelper.escapeIdentifier("foo`bar"));
    assertEquals("```leading`", DatabricksSqlHelper.escapeIdentifier("`leading"));
    assertEquals("`trailing```", DatabricksSqlHelper.escapeIdentifier("trailing`"));
  }

  @Test
  public void escapeIdentifier_withSpecialChars_escapesCorrectly() {
    assertEquals("`my schema.table`", DatabricksSqlHelper.escapeIdentifier("my schema.table"));
    assertEquals("`col-dash`", DatabricksSqlHelper.escapeIdentifier("col-dash"));
  }

  @Test
  public void executeQuery_withoutWarehouse_returnsEmpty() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    DatabricksHandle handle = new DatabricksHandle(client, null);
    List<List<String>> rows = DatabricksSqlHelper.executeQuery(handle, "SHOW SCHEMAS");
    assertTrue(rows.isEmpty());
  }
}
