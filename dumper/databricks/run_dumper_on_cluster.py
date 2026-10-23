# Databricks notebook source
# Copyright 2022-2025 Google LLC
# Copyright 2013-2021 CompilerWorks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# MAGIC %md
# MAGIC # Run the DWH migration dumper on this cluster (prototype)
# MAGIC
# MAGIC Runs the `databricks` connector of the dumper on the driver of this cluster. The
# MAGIC hive_metastore statements run on this cluster with `--cluster`, so no SQL warehouse is needed;
# MAGIC the Unity Catalog metadata is read with the REST API as usual.
# MAGIC
# MAGIC Prerequisites:
# MAGIC - The notebook is attached to an all-purpose cluster in dedicated access mode, on Databricks
# MAGIC   Runtime 16.4 LTS or above.
# MAGIC - The dumper zip that `./gradlew :dumper:app:distZip` builds from this branch,
# MAGIC   `dumper/app/build/distributions/app.zip`, is uploaded to a Unity Catalog volume.

# COMMAND ----------

dbutils.widgets.text("dumper_zip", "/Volumes/main/default/dumper/app.zip", "Dumper zip")
dbutils.widgets.text("output_dir", "/Volumes/main/default/dumper", "Output volume directory")
dbutils.widgets.text("catalogs", "", "Catalogs (--database), empty for all")

# COMMAND ----------

import os
import shutil
import subprocess
import zipfile

from databricks.sdk import WorkspaceClient

# The dumper runs on the local disk of the driver, because a volume doesn't support the random
# writes of a zip file.
WORK_DIR = "/local_disk0/tmp/dwh-migration-dumper"
OUTPUT_NAME = "dwh-migration-databricks-metadata.zip"
MAIN_CLASS = "com.google.edwmigration.dumper.application.dumper.Main"

# The notebook authentication of the SDK gives the workspace URL and a temporary token, which
# Databricks deletes when the notebook stops running.
workspace = WorkspaceClient()
host = workspace.config.host
token = workspace.config.authenticate()["Authorization"].split(" ", 1)[1]
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

shutil.rmtree(WORK_DIR, ignore_errors=True)
os.makedirs(WORK_DIR)
local_zip = os.path.join(WORK_DIR, "dwh-migration-dumper.zip")
shutil.copyfile(dbutils.widgets.get("dumper_zip"), local_zip)
with zipfile.ZipFile(local_zip) as distribution:
    distribution.extractall(WORK_DIR)
lib_dir = next(
    os.path.join(root, name)
    for root, names, _ in os.walk(WORK_DIR)
    for name in names
    if name == "lib"
)
java = shutil.which("java") or os.path.join(os.environ["JAVA_HOME"], "bin", "java")

output = os.path.join(WORK_DIR, OUTPUT_NAME)
command = [
    java, "-cp", os.path.join(lib_dir, "*"), MAIN_CLASS,
    "--connector", "databricks",
    "--url", host,
    "--cluster", cluster_id,
    "--output", output,
]
catalogs = dbutils.widgets.get("catalogs").strip()
if catalogs:
    command += ["--database", catalogs]

# The token goes in the environment rather than on the command line, so that the process list
# doesn't show it. Other DATABRICKS_ variables are dropped so they can't select another
# authentication method.
environment = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")}
environment["DATABRICKS_TOKEN"] = token
print(f"Running the dumper on cluster {cluster_id}: {' '.join(command)}")
process = subprocess.Popen(
    command, cwd=WORK_DIR, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    text=True,
)
for line in process.stdout:
    print(line, end="")
if process.wait() != 0:
    raise RuntimeError(f"The dumper failed with exit code {process.returncode}.")

# COMMAND ----------

target = os.path.join(dbutils.widgets.get("output_dir"), OUTPUT_NAME)
shutil.copyfile(output, target)
print(f"Saved the dumper output to {target}")
