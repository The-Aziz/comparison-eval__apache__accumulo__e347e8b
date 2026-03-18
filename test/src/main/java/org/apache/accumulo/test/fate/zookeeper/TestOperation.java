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

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOperation extends ManagerRepo {

  private static final Logger LOG = LoggerFactory.getLogger(TestOperation.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;

  public TestOperation(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {
    return Utils.reserveNamespace(manager, namespaceId, tid, LockType.READ, true,
        TableOperation.RENAME)
        + Utils.reserveTable(manager, tableId, tid, LockType.WRITE, true, TableOperation.RENAME);
  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    Utils.unreserveNamespace(manager, namespaceId, tid, LockType.READ);
    Utils.unreserveTable(manager, tableId, tid, LockType.WRITE);
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    LOG.debug("Entering call {}", FateTxId.formatTid(tid));
    try {
      // signal that call started
      if (FateITCoordination.callStarted != null) {
        FateITCoordination.callStarted.countDown();
      }
      // wait for the signal to exit the method
      if (FateITCoordination.finishCall != null) {
        FateITCoordination.finishCall.await();
      }
      return null;
    } finally {
      Utils.unreserveNamespace(manager, namespaceId, tid, LockType.READ);
      Utils.unreserveTable(manager, tableId, tid, LockType.WRITE);
      LOG.debug("Leaving call {}", FateTxId.formatTid(tid));
    }

  }

}
