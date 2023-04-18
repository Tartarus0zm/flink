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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TwoPhaseCatalogTable;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** An implementation of {@link TwoPhaseCatalogTable} for Hive to support atomic ctas. */
public class HiveTwoPhaseCatalogTable implements TwoPhaseCatalogTable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String hiveVersion;
    private final JobConfWrapper jobConfWrapper;

    private final ObjectPath tablePath;
    private final Table table;
    private final Map<String, String> options;
    private final String comments;
    private final String description;
    private final String detailedDesc;

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

    public HiveTwoPhaseCatalogTable(
            String hiveVersion,
            JobConfWrapper jobConfWrapper,
            ObjectPath tablePath,
            Table table,
            Map<String, String> options,
            String comments,
            String description,
            String detailedDesc,
            boolean ignoreIfExists) {
        this.hiveVersion = hiveVersion;
        this.jobConfWrapper = jobConfWrapper;
        this.tablePath = tablePath;
        this.table = table;
        this.options = options;
        this.comments = comments;
        this.description = description;
        this.detailedDesc = detailedDesc;
        this.ignoreIfExists = ignoreIfExists;
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

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String getComment() {
        return comments;
    }

    @Override
    public CatalogBaseTable copy() {
        return new HiveTwoPhaseCatalogTable(
                this.hiveVersion,
                new JobConfWrapper(new JobConf(this.jobConfWrapper.conf())),
                ObjectPath.fromString(tablePath.getFullName()),
                this.table.deepCopy(),
                new HashMap<>(this.options),
                this.comments,
                this.description,
                this.detailedDesc,
                this.ignoreIfExists);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.ofNullable(description);
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.ofNullable(detailedDesc);
    }

    @Override
    public boolean isPartitioned() {
        return table.getPartitionKeys().size() > 0;
    }

    @Override
    public List<String> getPartitionKeys() {
        return table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new HiveTwoPhaseCatalogTable(
                this.hiveVersion,
                new JobConfWrapper(new JobConf(this.jobConfWrapper.conf())),
                ObjectPath.fromString(tablePath.getFullName()),
                this.table.deepCopy(),
                options,
                this.comments,
                this.description,
                this.detailedDesc,
                this.ignoreIfExists);
    }
}
