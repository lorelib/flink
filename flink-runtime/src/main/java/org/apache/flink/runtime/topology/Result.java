/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.topology;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

/**
 * Represents a data set produced by a {@link Vertex} Each result is produced by one {@link Vertex}.
 * Each result can be consumed by multiple {@link Vertex}.
 *
 * TODO 代表着由Vertex产生的数据集，每个result由一个vertex产生，每个result可以被多个vertex消费
 */
public interface Result<
        VID extends VertexID,
        RID extends ResultID,
        V extends Vertex<VID, RID, V, R>,
        R extends Result<VID, RID, V, R>> {

    RID getId();

    ResultPartitionType getResultType();

    V getProducer();

    Iterable<? extends V> getConsumers();
}
