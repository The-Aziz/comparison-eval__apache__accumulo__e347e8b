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

import static org.apache.accumulo.core.Constants.ZTABLES;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;

/**
 * Utility methods for replication configuration and ZooKeeper paths.
 */
public class ReplicationConfigUtil {

  // Included for upgrade code usage any other usage post 3.0 should not be used.
  public static final TableId REPLICATION_ID = TableId.of("+rep");

  public static final String REPL_ITERATOR_PATTERN = "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner$";
  public static final String REPL_COLUMN_PATTERN =
      "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner\\.opt\\.columns$";

  public static final Pattern REPL_PATTERN = Pattern.compile("(" + REPL_ITERATOR_PATTERN + "|" + REPL_COLUMN_PATTERN + ")");

  /**
   * Utility method to build the ZooKeeper replication table path. The path resolves to
   * {@code /accumulo/INSTANCE_ID/tables/+rep}
   */
  public static String buildRepTablePath(final InstanceId iid) {
    return ZooUtil.getRoot(iid) + ZTABLES + "/" + REPLICATION_ID.canonical();
  }

  /**
   * Return a list of property keys that match replication iterator settings. This is specifically a
   * narrow filter to avoid potential matches with user define or properties that contain
   * replication in the property name (specifically table.file.replication which set hdfs block
   * replication.)
   */
  public static List<String> filterReplConfigKeys(Set<String> keys) {
    return keys.stream().filter(e -> REPL_PATTERN.matcher(e).find()).collect(Collectors.toList());
  }
}
