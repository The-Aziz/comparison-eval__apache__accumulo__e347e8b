/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.init;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This test is not thread-safe.
 */
public class InitializeTest {
  private InitializeTestEnv testEnv;

  @BeforeEach
  public void setUp() {
    testEnv = new InitializeTestEnv();
    testEnv.setUp();
  }

  @AfterEach
  public void tearDown() {
    testEnv.verifyAll();
  }

  @Test
  public void testIsInitialized_HasInstanceId() throws Exception {
    expect(testEnv.getFs().exists(anyObject(Path.class))).andReturn(true);
    testEnv.replayAll();
    assertTrue(Initialize.isInitialized(testEnv.getFs(), testEnv.getInitConfig()));
  }

  @Test
  public void testIsInitialized_HasDataVersion() throws Exception {
    expect(testEnv.getFs().exists(anyObject(Path.class))).andReturn(false);
    expect(testEnv.getFs().exists(anyObject(Path.class))).andReturn(true);
    testEnv.replayAll();
    assertTrue(Initialize.isInitialized(testEnv.getFs(), testEnv.getInitConfig()));
  }

  @Test
  public void testCheckInit_NoZK() throws Exception {
    expect(testEnv.getZoo().exists("/")).andReturn(false);
    testEnv.replayAll();
    assertThrows(IllegalStateException.class,
        () -> Initialize.checkInit(testEnv.getZoo(), testEnv.getFs(), testEnv.getInitConfig()));
  }

  @Test
  public void testCheckInit_AlreadyInit() throws Exception {
    expect(testEnv.getZoo().exists("/")).andReturn(true);
    expect(testEnv.getFs().exists(anyObject(Path.class))).andReturn(true);
    testEnv.replayAll();
    assertThrows(IOException.class,
        () -> Initialize.checkInit(testEnv.getZoo(), testEnv.getFs(), testEnv.getInitConfig()));
  }

  @Test
  public void testCheckInit_FSException() throws Exception {
    expect(testEnv.getZoo().exists("/")).andReturn(true);
    expect(testEnv.getFs().exists(anyObject(Path.class))).andThrow(new IOException());
    testEnv.replayAll();
    assertThrows(IOException.class,
        () -> Initialize.checkInit(testEnv.getZoo(), testEnv.getFs(), testEnv.getInitConfig()));
  }

  @Test
  public void testCheckInit_OK() throws Exception {
    expect(testEnv.getZoo().exists("/")).andReturn(true);
    // check for volumes initialized calls exists twice for each volume
    // once for instance_id, and once for version
    expect(testEnv.getFs().exists(anyObject(Path.class))).andReturn(false).times(4);
    testEnv.replayAll();
    Initialize.checkInit(testEnv.getZoo(), testEnv.getFs(), testEnv.getInitConfig());
  }
}
