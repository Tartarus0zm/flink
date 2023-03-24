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
 *
 */

package org.apache.flink.table.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.JobStatusHook;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** test. */
public class CtasJobStatusHook implements JobStatusHook {

    private static final Logger LOG = LoggerFactory.getLogger(CtasJobStatusHook.class);

    private Catalog catalog;
    //    private ResolvedCatalogTable table;
    //    private byte[] serializedCatalogTable;
    private Map<String, String> serializedCatalogTable;
    private ObjectPath objectPath;
    private boolean ignoreIfExists;

    public CtasJobStatusHook(
            Catalog catalog,
            ResolvedCatalogTable table,
            ObjectPath objectPath,
            boolean ignoreIfExists) {
        this.catalog = catalog;
        this.serializedCatalogTable = CatalogPropertiesUtil.serializeCatalogTable(table);
        this.objectPath = objectPath;
        this.ignoreIfExists = ignoreIfExists;
        //        try {
        //            // catalog table 2 hive table
        //            this.serializedCatalogTable = catalog.ser(objectPath, table);
        //        } catch (Exception e) {
        //            throw new RuntimeException(e);
        //        }
    }

    @Override
    public void onCreated(JobID jobId) {
        try {
            catalog.open();
            //            CatalogBaseTable catalogBaseTable = catalog.deser(objectPath,
            // serializedCatalogTable);
            catalog.createTable(
                    objectPath,
                    CatalogPropertiesUtil.deserializeCatalogTable(serializedCatalogTable),
                    //                    catalogBaseTable,
                    ignoreIfExists);
        } catch (Exception e) {
            // job should fail.
            String msg =
                    "Failed to create table "
                            + objectPath
                            + ", when job["
                            + jobId
                            + "] is starting!";
            LOG.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    @Override
    public void onFinished(JobID jobId) {
        // ignore
    }

    @Override
    public void onFailed(JobID jobId, Throwable throwable) {
        try {
            catalog.dropTable(objectPath, true);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to drop table " + objectPath + ", when job[" + jobId + "] is failed!",
                    e);
        }
    }

    @Override
    public void onCanceled(JobID jobId) {
        try {
            catalog.dropTable(objectPath, true);
        } catch (Exception e) {
            String msg =
                    "Failed to drop table " + objectPath + ", when job[" + jobId + "] is canceled!";
            LOG.warn(msg, e);
            throw new RuntimeException(msg, e);
        }
    }
}
