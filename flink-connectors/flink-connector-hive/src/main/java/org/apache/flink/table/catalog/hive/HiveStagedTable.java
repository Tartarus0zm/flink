/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.hive;

import org.apache.flink.connector.file.table.FileSystemCommitter;
import org.apache.flink.connector.file.table.FileSystemFactory;
import org.apache.flink.connector.file.table.PartitionCommitPolicy;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StagedTable;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/** An implementation of {@link StagedTable} for Hive to support atomic ctas. */
public class HiveStagedTable implements StagedTable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String hiveVersion;
    private final JobConfWrapper jobConfWrapper;

    private final Table table;

    private final boolean ignoreIfExists;

    private transient HiveMetastoreClientWrapper client;

    //
    private FileSystemFactory fsFactory;
    private TableMetaStoreFactory msFactory;
    private boolean overwrite;
    private Path tmpPath;
    private String[] partitionColumns;
    private boolean dynamicGrouped;
    private LinkedHashMap<String, String> staticPartitions;
    private ObjectIdentifier identifier;
    private PartitionCommitPolicyFactory partitionCommitPolicyFactory;

    public HiveStagedTable(
            String hiveVersion,
            JobConfWrapper jobConfWrapper,
            Table table,
            boolean ignoreIfExists) {
        this.hiveVersion = hiveVersion;
        this.jobConfWrapper = jobConfWrapper;
        this.table = table;
        this.ignoreIfExists = ignoreIfExists;
    }

    @Override
    public void begin() {
        // init hive metastore client
        client =
                HiveMetastoreClientFactory.create(
                        HiveConfUtils.create(jobConfWrapper.conf()), hiveVersion);
    }

    @Override
    public void commit() {
        try {
            // create table first
            client.createTable(table);

            try {
                List<PartitionCommitPolicy> policies = Collections.emptyList();
                if (partitionCommitPolicyFactory != null) {
                    policies =
                            partitionCommitPolicyFactory.createPolicyChain(
                                    Thread.currentThread().getContextClassLoader(),
                                    () -> {
                                        try {
                                            return fsFactory.create(tmpPath.toUri());
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                }

                FileSystemCommitter committer =
                        new FileSystemCommitter(
                                fsFactory,
                                msFactory,
                                overwrite,
                                tmpPath,
                                partitionColumns.length,
                                false,
                                identifier,
                                staticPartitions,
                                policies);
                committer.commitPartitions();
            } catch (Exception e) {
                throw new TableException("Exception in two phase commit", e);
            } finally {
                try {
                    fsFactory.create(tmpPath.toUri()).delete(tmpPath, true);
                } catch (IOException ignore) {
                }
            }
        } catch (AlreadyExistsException alreadyExistsException) {
            if (!ignoreIfExists) {
                throw new FlinkHiveException(alreadyExistsException);
            }
        } catch (Exception e) {
            throw new FlinkHiveException(e);
        } finally {
            client.close();
        }
    }

    @Override
    public void abort() {
        // do nothing
    }

    public void setFsFactory(FileSystemFactory fsFactory) {
        this.fsFactory = fsFactory;
    }

    public void setMsFactory(TableMetaStoreFactory msFactory) {
        this.msFactory = msFactory;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setTmpPath(Path tmpPath) {
        this.tmpPath = tmpPath;
    }

    public void setPartitionColumns(String[] partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public void setDynamicGrouped(boolean dynamicGrouped) {
        this.dynamicGrouped = dynamicGrouped;
    }

    public void setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
        this.staticPartitions = staticPartitions;
    }

    public void setIdentifier(ObjectIdentifier identifier) {
        this.identifier = identifier;
    }

    public void setPartitionCommitPolicyFactory(
            PartitionCommitPolicyFactory partitionCommitPolicyFactory) {
        this.partitionCommitPolicyFactory = partitionCommitPolicyFactory;
    }

    public Table getTable() {
        return table;
    }
}
