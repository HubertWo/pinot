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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.ReplicaGroupStrategyConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.utils.CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.CommonConstants.Segment.AssignmentStrategy;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OfflineReplicaGroupSegmentAssignmentTest {
  private static final int NUM_REPLICAS = 3;
  private static final String SEGMENT_NAME_PREFIX = "segment_";
  private static final int NUM_SEGMENTS = 90;
  private static final List<String> SEGMENTS =
      SegmentAssignmentTestUtils.getNameList(SEGMENT_NAME_PREFIX, NUM_SEGMENTS);
  private static final String INSTANCE_NAME_PREFIX = "instance_";
  private static final int NUM_INSTANCES = 18;
  private static final List<String> INSTANCES =
      SegmentAssignmentTestUtils.getNameList(INSTANCE_NAME_PREFIX, NUM_INSTANCES);
  private static final String RAW_TABLE_NAME_WITHOUT_PARTITION = "testTableWithoutPartition";
  private static final String INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITHOUT_PARTITION);
  private static final String RAW_TABLE_NAME_WITH_PARTITION = "testTableWithPartition";
  private static final String OFFLINE_TABLE_NAME_WITH_PARTITION =
      TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String INSTANCE_PARTITIONS_NAME_WITH_PARTITION =
      InstancePartitionsType.OFFLINE.getInstancePartitionsName(RAW_TABLE_NAME_WITH_PARTITION);
  private static final String PARTITION_COLUMN = "partitionColumn";
  private static final int NUM_PARTITIONS = 3;

  private SegmentAssignment _segmentAssignmentWithoutPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithoutPartition;
  private SegmentAssignment _segmentAssignmentWithPartition;
  private Map<InstancePartitionsType, InstancePartitions> _instancePartitionsMapWithPartition;

  @BeforeClass
  public void setUp() {
    TableConfig tableConfigWithoutPartitions =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITHOUT_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY).build();
    _segmentAssignmentWithoutPartition =
        SegmentAssignmentFactory.getSegmentAssignment(null, tableConfigWithoutPartitions);

    // {
    //   0_0=[instance_0, instance_1, instance_2, instance_3, instance_4, instance_5],
    //   0_1=[instance_6, instance_7, instance_8, instance_9, instance_10, instance_11],
    //   0_2=[instance_12, instance_13, instance_14, instance_15, instance_16, instance_17]
    // }
    InstancePartitions instancePartitionsWithoutPartition =
        new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITHOUT_PARTITION);
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    int instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      List<String> instancesForReplicaGroup = new ArrayList<>(numInstancesPerReplicaGroup);
      for (int i = 0; i < numInstancesPerReplicaGroup; i++) {
        instancesForReplicaGroup.add(INSTANCES.get(instanceIdToAdd++));
      }
      instancePartitionsWithoutPartition.setInstances(0, replicaGroupId, instancesForReplicaGroup);
    }
    _instancePartitionsMapWithoutPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitionsWithoutPartition);

    // Mock HelixManager
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStoreWithPartitions = mock(ZkHelixPropertyStore.class);
    List<ZNRecord> segmentZKMetadataZNRecords = new ArrayList<>(NUM_SEGMENTS);
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      OfflineSegmentZKMetadata segmentZKMetadata = new OfflineSegmentZKMetadata();
      segmentZKMetadata.setSegmentName(segmentName);
      int partitionId = segmentId % NUM_PARTITIONS;
      segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
          new ColumnPartitionMetadata(null, NUM_PARTITIONS, Collections.singleton(partitionId)))));
      ZNRecord segmentZKMetadataZNRecord = segmentZKMetadata.toZNRecord();
      when(propertyStoreWithPartitions.get(
          eq(ZKMetadataProvider.constructPropertyStorePathForSegment(OFFLINE_TABLE_NAME_WITH_PARTITION, segmentName)),
          any(), anyInt())).thenReturn(segmentZKMetadataZNRecord);
      segmentZKMetadataZNRecords.add(segmentZKMetadataZNRecord);
    }
    when(propertyStoreWithPartitions
        .getChildren(eq(ZKMetadataProvider.constructPropertyStorePathForResource(OFFLINE_TABLE_NAME_WITH_PARTITION)),
            any(), anyInt())).thenReturn(segmentZKMetadataZNRecords);
    HelixManager helixManagerWithPartitions = mock(HelixManager.class);
    when(helixManagerWithPartitions.getHelixPropertyStore()).thenReturn(propertyStoreWithPartitions);

    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setPartitionColumn(PARTITION_COLUMN);
    TableConfig tableConfigWithPartitions =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME_WITH_PARTITION)
            .setNumReplicas(NUM_REPLICAS)
            .setSegmentAssignmentStrategy(AssignmentStrategy.REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY).build();
    tableConfigWithPartitions.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);
    _segmentAssignmentWithPartition =
        SegmentAssignmentFactory.getSegmentAssignment(helixManagerWithPartitions, tableConfigWithPartitions);

    // {
    //   0_0=[instance_0, instance_1],
    //   0_1=[instance_6, instance_7],
    //   0_2=[instance_12, instance_13],
    //   1_0=[instance_2, instance_3],
    //   1_1=[instance_8, instance_9],
    //   1_2=[instance_14, instance_15],
    //   2_0=[instance_4, instance_5],
    //   2_1=[instance_10, instance_11],
    //   2_2=[instance_16, instance_17]
    // }
    InstancePartitions instancePartitionsWithPartition =
        new InstancePartitions(INSTANCE_PARTITIONS_NAME_WITH_PARTITION);
    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_REPLICAS;
    instanceIdToAdd = 0;
    for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
      for (int partitionId = 0; partitionId < NUM_PARTITIONS; partitionId++) {
        List<String> instancesForPartition = new ArrayList<>(numInstancesPerPartition);
        for (int i = 0; i < numInstancesPerPartition; i++) {
          instancesForPartition.add(INSTANCES.get(instanceIdToAdd++));
        }
        instancePartitionsWithPartition.setInstances(partitionId, replicaGroupId, instancesForPartition);
      }
    }
    _instancePartitionsMapWithPartition =
        Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitionsWithPartition);
  }

  @Test
  public void testFactory() {
    assertTrue(_segmentAssignmentWithoutPartition instanceof OfflineSegmentAssignment);
    assertTrue(_segmentAssignmentWithPartition instanceof OfflineSegmentAssignment);
  }

  @Test
  public void testAssignSegmentWithoutPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned = _segmentAssignmentWithoutPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithoutPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 should be assigned to instance 0, 6, 12
      // Segment 1 should be assigned to instance 1, 7, 13
      // Segment 2 should be assigned to instance 2, 8, 14
      // Segment 3 should be assigned to instance 3, 9, 15
      // Segment 4 should be assigned to instance 4, 10, 16
      // Segment 5 should be assigned to instance 5, 11, 17
      // Segment 6 should be assigned to instance 0, 6, 12
      // Segment 7 should be assigned to instance 1, 7, 13
      // ...
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int expectedAssignedInstanceId =
            segmentId % numInstancesPerReplicaGroup + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), INSTANCES.get(expectedAssignedInstanceId));
      }

      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testAssignSegmentWithPartition() {
    int numInstancesPerReplicaGroup = NUM_INSTANCES / NUM_REPLICAS;
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    int numInstancesPerPartition = numInstancesPerReplicaGroup / NUM_PARTITIONS;
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      String segmentName = SEGMENTS.get(segmentId);
      List<String> instancesAssigned = _segmentAssignmentWithPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithPartition);
      assertEquals(instancesAssigned.size(), NUM_REPLICAS);

      // Segment 0 (partition 0) should be assigned to instance 0, 6, 12
      // Segment 1 (partition 1) should be assigned to instance 2, 8, 14
      // Segment 2 (partition 2) should be assigned to instance 4, 10, 16
      // Segment 3 (partition 0) should be assigned to instance 1, 7, 13
      // Segment 4 (partition 1) should be assigned to instance 3, 9, 15
      // Segment 5 (partition 2) should be assigned to instance 5, 11, 17
      // Segment 6 (partition 0) should be assigned to instance 0, 6, 12
      // Segment 7 (partition 1) should be assigned to instance 2, 8, 14
      // ...
      int partitionId = segmentId % NUM_PARTITIONS;
      for (int replicaGroupId = 0; replicaGroupId < NUM_REPLICAS; replicaGroupId++) {
        int expectedAssignedInstanceId =
            (segmentId % numInstancesPerReplicaGroup) / NUM_PARTITIONS + partitionId * numInstancesPerPartition
                + replicaGroupId * numInstancesPerReplicaGroup;
        assertEquals(instancesAssigned.get(replicaGroupId), INSTANCES.get(expectedAssignedInstanceId));
      }

      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }
  }

  @Test
  public void testTableBalancedWithoutPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithoutPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithoutPartition);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // There should be 90 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 15 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(_segmentAssignmentWithoutPartition
        .rebalanceTable(currentAssignment, _instancePartitionsMapWithoutPartition, null), currentAssignment);
  }

  @Test
  public void testTableBalancedWithPartition() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    for (String segmentName : SEGMENTS) {
      List<String> instancesAssigned = _segmentAssignmentWithPartition
          .assignSegment(segmentName, currentAssignment, _instancePartitionsMapWithPartition);
      currentAssignment.put(segmentName,
          SegmentAssignmentUtils.getInstanceStateMap(instancesAssigned, SegmentOnlineOfflineStateModel.ONLINE));
    }

    // There should be 90 segments assigned
    assertEquals(currentAssignment.size(), NUM_SEGMENTS);
    // Each segment should have 3 replicas
    for (Map<String, String> instanceStateMap : currentAssignment.values()) {
      assertEquals(instanceStateMap.size(), NUM_REPLICAS);
    }
    // Each instance should have 15 segments assigned
    int[] numSegmentsAssignedPerInstance =
        SegmentAssignmentUtils.getNumSegmentsAssignedPerInstance(currentAssignment, INSTANCES);
    int[] expectedNumSegmentsAssignedPerInstance = new int[NUM_INSTANCES];
    int numSegmentsPerInstance = NUM_SEGMENTS * NUM_REPLICAS / NUM_INSTANCES;
    Arrays.fill(expectedNumSegmentsAssignedPerInstance, numSegmentsPerInstance);
    assertEquals(numSegmentsAssignedPerInstance, expectedNumSegmentsAssignedPerInstance);
    // Current assignment should already be balanced
    assertEquals(
        _segmentAssignmentWithPartition.rebalanceTable(currentAssignment, _instancePartitionsMapWithPartition, null),
        currentAssignment);
  }
}