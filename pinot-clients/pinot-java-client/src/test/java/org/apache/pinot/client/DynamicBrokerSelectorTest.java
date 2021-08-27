/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class DynamicBrokerSelectorTest {

  @Mock
  private ExternalViewReader _mockExternalViewReader;

  @Mock
  private ZkClient _mockZkClient;

  private DynamicBrokerSelector _dynamicBrokerSelectorUnderTest;

  private static final String ZK_SERVER = "zkServers";
  private static final Map<String, List<String>> TABLE_TO_BROKER_LIST_MAP = ImmutableMap.of(
          "table1", Collections.singletonList("broker1")
  );

  @BeforeMethod
  public void setUp() throws Exception {
    openMocks(this);
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(TABLE_TO_BROKER_LIST_MAP);
    _dynamicBrokerSelectorUnderTest = Mockito.spy(new DynamicBrokerSelector(ZK_SERVER) {
      @Override
      protected ExternalViewReader getEvReader(ZkClient zkClient) {
        return _mockExternalViewReader;
      }

      @Override
      protected ZkClient getZkClient(String zkServers) {
        return _mockZkClient;
      }
    });
  }

  @Test
  public void testHandleDataChange() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    verify(_mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test
  public void testHandleDataDeleted() {
    _dynamicBrokerSelectorUnderTest.handleDataDeleted("dataPath");

    verify(_mockExternalViewReader, times(2)).getTableToBrokersMap();
  }

  @Test
  public void testSelectBroker() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

     String result = _dynamicBrokerSelectorUnderTest.selectBroker("table1");

    assertEquals("broker1", result);
  }

  @Test
  public void testSelectBrokerForNullTable() {
    _dynamicBrokerSelectorUnderTest.handleDataChange("dataPath", "data");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker(null);

    assertEquals("broker1", result);
  }

  @Test
  public void testSelectBrokerForNullTableAndEmptyBrokerListRef() {
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(Collections.emptyMap());
    _dynamicBrokerSelectorUnderTest.handleDataChange("dummy-data-path", "dummy-date");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker(null);

    assertNull(result);
  }

  @Test
  public void testSelectBrokerForNonNullTableAndEmptyBrokerListRef() {
    when(_mockExternalViewReader.getTableToBrokersMap()).thenReturn(Collections.emptyMap());
    _dynamicBrokerSelectorUnderTest.handleDataChange("dummy-data-path", "dummy-date");

    String result = _dynamicBrokerSelectorUnderTest.selectBroker("dummyTableName");

    assertNull(result);
  }

  @Test
  public void testGetBrokers() {
    List<String> actualBrokers = _dynamicBrokerSelectorUnderTest.getBrokers();

    assertEquals(actualBrokers, ImmutableList.of("broker1"));
  }

  @Test
  public void testCloseZkClient() {
    _dynamicBrokerSelectorUnderTest.close();

    Mockito.verify(_mockZkClient, times(1)).close();
  }

}
