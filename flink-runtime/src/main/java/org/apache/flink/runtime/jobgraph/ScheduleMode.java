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

package org.apache.flink.runtime.jobgraph;

/** The ScheduleMode decides how tasks of an execution graph are started. */
// TODO 调度模型ScheduleMode决定着如何启动执行图的任务
public enum ScheduleMode {
    /**
     * Schedule tasks lazily from the sources. Downstream tasks are started once their input data
     * are ready
     *
     * TODO 从数据源懒调度任务，直到输入数据准备好，下游算子才开始执行
     */
    LAZY_FROM_SOURCES(true),

    /**
     * Same as LAZY_FROM_SOURCES just with the difference that it uses batch slot requests which
     * support the execution of jobs with fewer slots than requested. However, the user needs to
     * make sure that the job does not contain any pipelined shuffles (every pipelined region can be
     * executed with a single slot).
     *
     * TODO 跟LAZY_FROM_SOURCES相似，不同的是它用于批量资源申请，并且支持当申请的资源不够时也能执行job；
     *  确保job没有任何shuffles, 每块区域可以被一个单独的slot处理
     */
    LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST(true),

    /** Schedules all tasks immediately. */
    // TODO 立即启动所有任务
    EAGER(false);

    private final boolean allowLazyDeployment;

    ScheduleMode(boolean allowLazyDeployment) {
        this.allowLazyDeployment = allowLazyDeployment;
    }

    /** Returns whether we are allowed to deploy consumers lazily. */
    public boolean allowLazyDeployment() {
        return allowLazyDeployment;
    }
}
