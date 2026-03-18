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
package org.apache.accumulo.manager.upgrade;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStore;

/**
 * Helper class for managing the test environment and mocks for Upgrader10to11Test.
 */
public class Upgrader10to11TestContext {

  private final InstanceId instanceId;
  private final ServerContext context;
  private final ZooReaderWriter zrw;
  private final PropStore propStore;

  public Upgrader10to11TestContext() {
    this.instanceId = InstanceId.of(UUID.randomUUID());
    this.context = createMock(ServerContext.class);
    this.zrw = createMock(ZooReaderWriter.class);
    this.propStore = createMock(PropStore.class);

    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public ServerContext getContext() {
    return context;
  }

  public ZooReaderWriter getZooReaderWriter() {
    return zrw;
  }

  public final PropStore getPropStore() {
    return propStore;
  }

  public void replayAll() {
    replay(context, zrw, propStore);
  }

  public void verifyAll() {
    verify(context, zrw, propStore);
  }
}
