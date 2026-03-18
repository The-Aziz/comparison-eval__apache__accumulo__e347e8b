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

import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestOperationFails extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(TestOperationFails.class);
  public static final int TOTAL_NUM_OPS = 3;
  private final int opNum;
  private final String opName;
  private final ExceptionLocation location;

  public TestOperationFails(int opNum, ExceptionLocation location) {
    this.opNum = opNum;
    this.opName = "OP" + opNum;
    this.location = location;
  }

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    LOG.debug("{} {} Entered isReady()", opName, FateTxId.formatTid(tid));
    if (location == ExceptionLocation.IS_READY) {
      if (opNum < TOTAL_NUM_OPS) {
        return 0;
      } else {
        throw new Exception(
            opName + " " + FateTxId.formatTid(tid) + " isReady() failed - this is expected");
      }
    } else {
      return 0;
    }
  }

  @Override
  public void undo(long tid, Manager environment) throws Exception {
    LOG.debug("{} {} Entered undo()", opName, FateTxId.formatTid(tid));
    if (FateITCoordination.undoOrder != null) {
      FateITCoordination.undoOrder.add(opName);
    }
    if (FateITCoordination.undoLatch != null) {
      FateITCoordination.undoLatch.countDown();
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager environment) throws Exception {
    LOG.debug("{} {} Entered call()", opName, FateTxId.formatTid(tid));
    if (location == ExceptionLocation.CALL) {
      if (opNum < TOTAL_NUM_OPS) {
        return new TestOperationFails(opNum + 1, location);
      } else {
        throw new Exception(
            opName + " " + FateTxId.formatTid(tid) + " call() failed - this is expected");
      }
    } else {
      return new TestOperationFails(opNum + 1, location);
    }
  }
}
