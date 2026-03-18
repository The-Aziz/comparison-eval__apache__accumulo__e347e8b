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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZTABLE_STATE;
import static org.apache.accumulo.manager.upgrade.ReplicationConfigUtil.buildRepTablePath;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Upgrader10to11Test {
  private static final Logger log = LoggerFactory.getLogger(Upgrader10to11Test.class);

  private Upgrader10to11TestContext testContext;

  @BeforeEach
  public void init() {
    testContext = new Upgrader10to11TestContext();
  }

  @Test
  void upgradeZooKeeperGoPath() throws Exception {
    ServerContext context = testContext.getContext();
    ZooReaderWriter zrw = testContext.getZooReaderWriter();
    PropStore propStore = testContext.getPropStore();
    InstanceId instanceId = testContext.getInstanceId();

    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(zrw.exists(buildRepTablePath(instanceId))).andReturn(true).once();
    expect(zrw.getData(buildRepTablePath(instanceId) + ZTABLE_STATE))
        .andReturn(TableState.OFFLINE.name().getBytes(UTF_8)).once();
    zrw.recursiveDelete(buildRepTablePath(instanceId), ZooUtil.NodeMissingPolicy.SKIP);
    expectLastCall().once();

    expect(propStore.get(TablePropKey.of(instanceId, AccumuloTable.METADATA.tableId())))
        .andReturn(new VersionedProperties()).once();

    testContext.replayAll();

    Upgrader10to11 upgrader = new Upgrader10to11();
    upgrader.upgradeZookeeper(context);

    testContext.verifyAll();
  }

  @Test
  void upgradeZookeeperNoReplTableNode() throws Exception {
    ServerContext context = testContext.getContext();
    ZooReaderWriter zrw = testContext.getZooReaderWriter();
    InstanceId instanceId = testContext.getInstanceId();

    expect(zrw.exists(buildRepTablePath(instanceId))).andReturn(false).once();
    testContext.replayAll();

    Upgrader10to11 upgrader = new Upgrader10to11();
    upgrader.upgradeZookeeper(context);

    testContext.verifyAll();
  }

  @Test
  void checkReplicationStateOnline() throws Exception {
    ServerContext context = testContext.getContext();
    ZooReaderWriter zrw = testContext.getZooReaderWriter();
    InstanceId instanceId = testContext.getInstanceId();

    expect(zrw.exists(buildRepTablePath(instanceId))).andReturn(true).once();
    expect(zrw.getData(buildRepTablePath(instanceId) + ZTABLE_STATE))
        .andReturn(TableState.ONLINE.name().getBytes(UTF_8)).anyTimes();
    testContext.replayAll();

    Upgrader10to11 upgrader = new Upgrader10to11();
    assertThrows(IllegalStateException.class, () -> upgrader.upgradeZookeeper(context));

    testContext.verifyAll();
  }

  @Test
  void checkReplicationStateNoNode() throws Exception {
    ServerContext context = testContext.getContext();
    ZooReaderWriter zrw = testContext.getZooReaderWriter();
    InstanceId instanceId = testContext.getInstanceId();

    expect(zrw.exists(buildRepTablePath(instanceId))).andReturn(true).once();
    expect(zrw.getData(buildRepTablePath(instanceId) + ZTABLE_STATE))
        .andThrow(new KeeperException.NoNodeException("force no node exception")).anyTimes();
    testContext.replayAll();

    Upgrader10to11 upgrader = new Upgrader10to11();
    assertThrows(IllegalStateException.class, () -> upgrader.upgradeZookeeper(context));

    testContext.verifyAll();
  }

  @Test
  public void filterTest() {
    Map<String,String> entries = new HashMap<>();
    entries.put("table.file.compress.blocksize", "32K");
    entries.put("table.file.replication", "5");
    entries.put("table.group.server", "file,log,srv,future");
    entries.put("table.iterator.majc.bulkLoadFilter",
        "20,org.apache.accumulo.server.iterators.MetadataBulkLoadFilter");
    entries.put("table.iterator.majc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.majc.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.majc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");
    entries.put("table.iterator.majc.vers.opt.maxVersions", "1");
    entries.put("table.iterator.minc.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.minc.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.minc.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");
    entries.put("table.iterator.minc.vers.opt.maxVersions", "1");
    entries.put("table.iterator.scan.replcombiner",
        "9,org.apache.accumulo.server.replication.StatusCombiner");
    entries.put("table.iterator.scan.replcombiner.opt.columns", "stat");
    entries.put("table.iterator.scan.vers",
        "10,org.apache.accumulo.core.iterators.user.VersioningIterator");

    List<String> filtered = ReplicationConfigUtil.filterReplConfigKeys(entries.keySet());

    assertEquals(6, filtered.size());
    log.info("F:{}", filtered);
  }
}
