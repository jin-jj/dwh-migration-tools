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

import java.io.IOException;
import java.util.Locale;
import javax.annotation.CheckForNull;

/** Exception thrown when a Databricks SQL Statement Execution fails. */
final class DatabricksSqlException extends IOException {

  private final String sqlState;
  private final String errorCode;

  DatabricksSqlException(
      String message, @CheckForNull String sqlState, @CheckForNull String errorCode) {
    super(message);
    this.sqlState = sqlState;
    this.errorCode = errorCode;
  }

  DatabricksSqlException(String message, Throwable cause) {
    super(message, cause);
    this.sqlState = null;
    this.errorCode = null;
  }

  @CheckForNull
  String sqlState() {
    return sqlState;
  }

  @CheckForNull
  String errorCode() {
    return errorCode;
  }

  boolean isPermissionDenied() {
    if ("42501".equals(sqlState)) {
      return true;
    }
    if (errorCode != null) {
      String upperCode = errorCode.toUpperCase(Locale.ROOT);
      if (upperCode.contains("PERMISSION_DENIED")
          || upperCode.contains("INSUFFICIENT_PERMISSIONS")) {
        return true;
      }
    }
    String msg = getMessage();
    if (msg != null) {
      String upperMsg = msg.toUpperCase(Locale.ROOT);
      if (upperMsg.contains("SQLSTATE: 42501")
          || upperMsg.contains("INSUFFICIENT_PERMISSIONS")
          || upperMsg.contains("PERMISSION_DENIED")
          || upperMsg.contains("INSUFFICIENT PRIVILEGES")) {
        return true;
      }
    }
    return false;
  }
}
