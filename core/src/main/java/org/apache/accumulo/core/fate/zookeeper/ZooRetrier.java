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
package org.apache.accumulo.core.fate.zookeeper;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooRetrier {
  private static final Logger log = LoggerFactory.getLogger(ZooRetrier.class);

  private static final RetryFactory RETRY_FACTORY =
      Retry.builder().maxRetries(10).retryAfter(Duration.ofMillis(250))
          .incrementBy(Duration.ofMillis(250)).maxWait(Duration.ofMinutes(2)).backOffFactor(1.5)
          .logInterval(Duration.ofMinutes(3)).createFactory();

  public interface ZooKeeperAction<R> {
    R apply(ZooKeeper zk) throws KeeperException, InterruptedException;
  }

  public interface ZooKeeperMutatorAction<R> {
    R apply(ZooKeeper zk)
        throws KeeperException, InterruptedException, AcceptableThriftTableOperationException;
  }

  public RetryFactory getRetryFactory() {
    return RETRY_FACTORY;
  }

  public <R> R retryLoop(ZooKeeperAction<R> zkf, Predicate<KeeperException> alwaysRetryCondition,
      Supplier<ZooKeeper> zkSupplier) throws KeeperException, InterruptedException {
    try {
      return retryLoopMutator(zkf::apply, alwaysRetryCondition, zkSupplier);
    } catch (AcceptableThriftTableOperationException e) {
      throw new AssertionError("Not possible; " + ZooKeeperAction.class.getName() + " can't throw "
          + AcceptableThriftTableOperationException.class.getName());
    }
  }

  public <R> R retryLoopMutator(ZooKeeperMutatorAction<R> zkf,
      Predicate<KeeperException> alwaysRetryCondition, Supplier<ZooKeeper> zkSupplier)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    requireNonNull(zkf);
    requireNonNull(alwaysRetryCondition);
    var retries = getRetryFactory().createRetry();
    while (true) {
      try {
        return zkf.apply(zkSupplier.get());
      } catch (KeeperException e) {
        if (alwaysRetryCondition.test(e)) {
          retries.waitForNextAttempt(log,
              "attempting to communicate with zookeeper after exception that always requires retry: "
                  + e.getMessage());
          continue;
        } else if (useRetryForTransient(retries, e)) {
          continue;
        }
        throw e;
      }
    }
  }

  private static boolean useRetryForTransient(Retry retries, KeeperException e)
      throws KeeperException, InterruptedException {
    final Code c = e.code();
    if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
      log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
      if (retries.canRetry()) {
        retries.useRetry();
        retries.waitForNextAttempt(log,
            "attempting to communicate with zookeeper after exception: " + e.getMessage());
        return true;
      }
      log.error("Retry attempts ({}) exceeded trying to communicate with ZooKeeper",
          retries.retriesCompleted());
    }
    return false;
  }
}
