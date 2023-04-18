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

package org.apache.flink.table.execution;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.table.catalog.TwoPhaseCatalogTable;

/**
 * This Hook is used to implement the Flink CTAS atomicity semantics, calling the corresponding API
 * of {@link TwoPhaseCatalogTable} at different stages of the job.
 */
public class CtasJobStatusHook implements JobStatusHook {

    private final TwoPhaseCatalogTable twoPhaseCatalogTable;

    public CtasJobStatusHook(TwoPhaseCatalogTable twoPhaseCatalogTable) {
        this.twoPhaseCatalogTable = twoPhaseCatalogTable;
    }

    @Override
    public void onCreated(JobID jobId) {
        twoPhaseCatalogTable.begin();
    }

    @Override
    public void onFinished(JobID jobId) {
        twoPhaseCatalogTable.commit();
    }

    @Override
    public void onFailed(JobID jobId, Throwable throwable) {
        twoPhaseCatalogTable.abort();
    }

    @Override
    public void onCanceled(JobID jobId) {
        twoPhaseCatalogTable.abort();
    }
}
