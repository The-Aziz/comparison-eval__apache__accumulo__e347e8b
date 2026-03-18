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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;

/**
 * Encapsulates the test environment and mocks for Initialize tests.
 */
public class InitializeTestEnv {
  private Configuration conf;
  private VolumeManager fs;
  private SiteConfiguration sconf;
  private ZooReaderWriter zoo;
  private InitialConfiguration initConfig;

  public void setUp() {
    conf = new Configuration(false);
    fs = createMock(VolumeManager.class);
    sconf = createMock(SiteConfiguration.class);
    initConfig = new InitialConfiguration(conf, sconf);
    expect(sconf.get(Property.INSTANCE_VOLUMES))
        .andReturn("hdfs://foo/accumulo,hdfs://bar/accumulo").anyTimes();
    expect(sconf.get(Property.INSTANCE_SECRET))
        .andReturn(Property.INSTANCE_SECRET.getDefaultValue()).anyTimes();
    expect(sconf.get(Property.INSTANCE_ZK_HOST)).andReturn("zk1").anyTimes();
    zoo = createMock(ZooReaderWriter.class);
  }

  public void replayAll() {
    replay(sconf, zoo, fs);
  }

  public void verifyAll() {
    verify(sconf, zoo, fs);
  }

  public Configuration getConf() {
    return conf;
  }

  public VolumeManager getFs() {
    return fs;
  }

  public SiteConfiguration getSconf() {
    return sconf;
  }

  public ZooReaderWriter getZoo() {
    return zoo;
  }

  public InitialConfiguration getInitConfig() {
    return initConfig;
  }
}
