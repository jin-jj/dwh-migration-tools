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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.checkCluster;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.DataSecurityMode;
import com.databricks.sdk.service.compute.State;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TablesFormat;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksMetadataConnectorTest {

  private static final String CLUSTER_ID = "0123-456789-abcdefgh";

  private final DatabricksMetadataConnector connector = new DatabricksMetadataConnector();

  @Test
  public void validate_acceptsEitherWarehouseOrCluster() {
    connector.validate(arguments("--database", "hive_metastore", "--cluster", CLUSTER_ID));
    connector.validate(arguments("--database", "hive_metastore", "--warehouse", "wh-1"));

    assertThrows(
        MetadataDumperUsageException.class,
        () -> connector.validate(arguments("--warehouse", "wh-1", "--cluster", CLUSTER_ID)));
    assertThrows(
        MetadataDumperUsageException.class,
        () -> connector.validate(arguments("--database", "hive_metastore")));
  }

  @Test
  public void addTasksTo_addsHiveMetastoreTasksWithCluster() {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments("--cluster", CLUSTER_ID));

    boolean hasHiveMetastoreTables = false;
    for (Task<?> task : tasks) {
      hasHiveMetastoreTables |= TablesFormat.HMS_ZIP_ENTRY_NAME.equals(task.getTargetPath());
    }
    assertTrue(tasks.toString(), hasHiveMetastoreTables);
  }

  @Test
  public void checkCluster_acceptsRunningClusterOnRuntime16Point2OrAbove() {
    checkCluster(cluster(State.RUNNING, "16.2.x-scala2.12"));
    checkCluster(cluster(State.RUNNING, "17.3.x-photon-scala2.13"));
  }

  @Test
  public void checkCluster_rejectsOlderRuntimeAndStoppedCluster() {
    MetadataDumperUsageException olderRuntime =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> checkCluster(cluster(State.RUNNING, "15.4.x-scala2.12")));
    assertTrue(olderRuntime.getMessage(), olderRuntime.getMessage().contains("15.4.x-scala2.12"));

    MetadataDumperUsageException stopped =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> checkCluster(cluster(State.TERMINATED, "16.4.x-scala2.12")));
    assertTrue(stopped.getMessage(), stopped.getMessage().contains(State.TERMINATED.toString()));
  }

  private static ClusterDetails cluster(State state, String sparkVersion) {
    return new ClusterDetails()
        .setClusterId(CLUSTER_ID)
        .setState(state)
        .setSparkVersion(sparkVersion)
        .setDataSecurityMode(DataSecurityMode.SINGLE_USER);
  }

  private static ConnectorArguments arguments(String... arguments) {
    return ConnectorArguments.create(
        ImmutableList.<String>builder()
            .add("--connector", DatabricksMetadataConnector.CONNECTOR_NAME)
            .add("--url", "https://example.cloud.databricks.com")
            .add(arguments)
            .build());
  }
}
