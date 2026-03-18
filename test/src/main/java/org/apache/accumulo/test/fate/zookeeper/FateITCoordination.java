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
package org.apache.accumulo.test.fate.zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Coordination utility to provide access to shared state (latches, lists) for FATE integration
 * tests. This is necessary because FATE Repo objects are serialized and then executed in a
 * different thread/environment, making standard dependency injection of non-serializable objects
 * (like CountDownLatch) via the constructor difficult.
 */
public class FateITCoordination {
  public static CountDownLatch callStarted;
  public static CountDownLatch finishCall;
  public static CountDownLatch undoLatch;
  public static List<String> undoOrder;
}
