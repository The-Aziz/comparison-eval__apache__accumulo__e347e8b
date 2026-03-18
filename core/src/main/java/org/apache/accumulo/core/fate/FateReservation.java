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
package org.apache.accumulo.core.fate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.apache.accumulo.core.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.core.util.CountDownTimer;

class FateReservation {
  private String lastReserved = "";
  private final Set<Long> reserved = new HashSet<>();
  private final Map<Long,CountDownTimer> deferred = new HashMap<>();
  private long statusChangeEvents = 0;
  private int reservationsWaiting = 0;

  public synchronized void reserve(long tid) {
    reservationsWaiting++;
    try {
      while (reserved.contains(tid)) {
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }

      reserved.add(tid);
    } finally {
      reservationsWaiting--;
    }
  }

  public synchronized boolean tryReserve(long tid) {
    if (!reserved.contains(tid)) {
      reserve(tid);
      return true;
    }
    return false;
  }

  public synchronized void unreserve(long tid) {
    if (!reserved.remove(tid)) {
      throw new IllegalStateException(
          "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
    }

    // do not want this unreserve to wake up threads in reserve()... this leads to infinite
    // loop when tx is stuck in NEW...
    // only do this when something external has called reserve(tid)...
    if (reservationsWaiting > 0) {
      this.notifyAll();
    }
  }

  public synchronized void unreserve(long tid, Duration deferTime) {
    if (deferTime.isNegative()) {
      throw new IllegalArgumentException("deferTime < 0 : " + deferTime);
    }

    if (!reserved.remove(tid)) {
      throw new IllegalStateException(
          "Tried to unreserve id that was not reserved " + FateTxId.formatTid(tid));
    }

    if (deferTime.compareTo(Duration.ZERO) > 0) {
      deferred.put(tid, CountDownTimer.startNew(deferTime));
    }

    this.notifyAll();
  }

  public synchronized void verifyReserved(long tid) {
    if (!reserved.contains(tid)) {
      throw new IllegalStateException(
          "Tried to operate on unreserved transaction " + FateTxId.formatTid(tid));
    }
  }

  public TStatus waitForStatusChange(long tid, EnumSet<TStatus> expected,
      Supplier<TStatus> statusGetter) {
    while (true) {
      long events;
      synchronized (this) {
        events = statusChangeEvents;
      }

      TStatus status = statusGetter.get();
      if (expected.contains(status)) {
        return status;
      }

      synchronized (this) {
        if (events == statusChangeEvents) {
          try {
            this.wait(5000);
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
        }
      }
    }
  }

  public synchronized void notifyStatusChange() {
    statusChangeEvents++;
    this.notifyAll();
  }

  public long reserve(Supplier<List<String>> txdirsSupplier, ToLongFunction<String> tidParser,
      Function<Long,TStatus> statusGetter, Consumer<Long> unreserver) {
    while (true) {
      long events;
      synchronized (this) {
        events = statusChangeEvents;
      }

      List<String> txdirs = txdirsSupplier.get();
      Collections.sort(txdirs);

      synchronized (this) {
        if (!txdirs.isEmpty() && txdirs.get(txdirs.size() - 1).compareTo(lastReserved) <= 0) {
          lastReserved = "";
        }
      }

      for (String txdir : txdirs) {
        long tid = tidParser.applyAsLong(txdir);

        synchronized (this) {
          if (txdir.compareTo(lastReserved) <= 0) {
            continue;
          }

          if (deferred.containsKey(tid)) {
            if (deferred.get(tid).isExpired()) {
              deferred.remove(tid);
            } else {
              continue;
            }
          }
          if (reserved.contains(tid)) {
            continue;
          } else {
            reserved.add(tid);
            lastReserved = txdir;
          }
        }

        try {
          TStatus status = statusGetter.apply(tid);
          if (status == TStatus.SUBMITTED || status == TStatus.IN_PROGRESS
              || status == TStatus.FAILED_IN_PROGRESS) {
            return tid;
          } else {
            unreserver.accept(tid);
          }
        } catch (Exception e) {
          unreserver.accept(tid);
          throw e;
        }
      }

      synchronized (this) {
        if (events == statusChangeEvents) {
          if (deferred.isEmpty()) {
            try {
              this.wait(5000);
            } catch (InterruptedException e) {
              throw new IllegalStateException(e);
            }
          } else {
            long minWait = deferred.values().stream()
                .mapToLong(timer -> timer.timeLeft(MILLISECONDS)).min().orElseThrow();
            if (minWait > 0) {
              try {
                this.wait(Math.min(minWait, 5000));
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            }
          }
        }
      }
    }
  }
}
