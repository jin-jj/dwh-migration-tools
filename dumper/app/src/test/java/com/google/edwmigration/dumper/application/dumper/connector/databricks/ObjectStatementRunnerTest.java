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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.ObjectStatementRunner.runInOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ObjectStatementRunnerTest {

  @Test
  public void runInOrder_skipsFailedObjectsAndKeepsInputOrder() throws IOException {
    ImmutableList<String> inputs = ImmutableList.of("a", "b", "c", "d");
    ImmutableSet<String> checkedFailures = ImmutableSet.of("b");
    ImmutableSet<String> uncheckedFailures = ImmutableSet.of("c");
    List<String> consumed = new ArrayList<>();

    runInOrder(
        "TEST",
        inputs,
        input -> input,
        input -> {
          if (checkedFailures.contains(input)) {
            throw new IOException("Broken " + input);
          }
          if (uncheckedFailures.contains(input)) {
            throw new UncheckedIOException(new IOException("Broken " + input));
          }
          return input.toUpperCase(Locale.ROOT);
        },
        (input, result) -> consumed.add(input + "=" + result));

    List<String> expected = new ArrayList<>();
    for (String input : inputs) {
      if (!checkedFailures.contains(input) && !uncheckedFailures.contains(input)) {
        expected.add(input + "=" + input.toUpperCase(Locale.ROOT));
      }
    }
    assertEquals(expected, consumed);
  }

  @Test
  public void runInOrder_throwsFirstFailureWhenEveryStatementFails() {
    ImmutableList<String> inputs = ImmutableList.of("a", "b");
    List<IOException> failures = new ArrayList<>();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runInOrder(
                    "TEST",
                    inputs,
                    input -> input,
                    input -> {
                      IOException failure = new IOException("Broken " + input);
                      failures.add(failure);
                      throw failure;
                    },
                    (input, result) -> fail("No statement succeeded.")));

    assertSame(failures.get(0), thrown.getCause());
    assertTrue(thrown.getMessage().contains("all " + inputs.size() + " objects"));
  }

  @Test
  public void runInOrder_acceptsNoInputs() throws IOException {
    runInOrder(
        "TEST",
        ImmutableList.<String>of(),
        input -> input,
        input -> input,
        (input, result) -> fail("There are no inputs."));
  }

  @Test
  public void runInOrder_propagatesRuntimeExceptions() {
    IllegalStateException bug = new IllegalStateException("Bug");

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                runInOrder(
                    "TEST",
                    ImmutableList.of("a", "b"),
                    input -> input,
                    input -> {
                      throw bug;
                    },
                    (input, result) -> fail("No statement succeeded.")));

    assertSame(bug, thrown);
  }
}
