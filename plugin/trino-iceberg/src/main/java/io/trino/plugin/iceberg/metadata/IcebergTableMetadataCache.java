/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.FileIO;
import org.weakref.jmx.Managed;

import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergUtil.getFileModifiedTimePathDomain;
import static io.trino.plugin.iceberg.IcebergUtil.getModificationTime;
import static io.trino.plugin.iceberg.IcebergUtil.getPathDomain;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;

public class IcebergTableMetadataCache
{
    private final TypeManager typeManager;
    private final LoadingCache<String, TableMetadata> metadataCache;
    private final LoadingCache<CacheKey, IcebergTableStatistics> statisticsCache;

    @Inject
    public IcebergTableMetadataCache(TrinoFileSystemFactory fileSystemFactory, IcebergConfig icebergConfig, TypeManager typeManager)
    {
        ConnectorSession session = new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return "query_for_table_metadata_caching";
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return ConnectorIdentity.forUser("hdfs").build();
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return null;
            }

            @Override
            public Locale getLocale()
            {
                return null;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public Instant getStart()
            {
                return null;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                return null;
            }
        };

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        FileIO fileIO = new ForwardingFileIo(fileSystem);
        this.typeManager = typeManager;
        this.metadataCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .expireAfterAccess(icebergConfig.getMetadataCacheTtl().toJavaTime())
                        .maximumSize(icebergConfig.getCacheSize()),
                new CacheLoader<>()
                {
                    @Override
                    public TableMetadata load(String location)
                            throws Exception
                    {
                        return TableMetadataParser.read(fileIO, fileIO.newInputFile(location));
                    }
                });

        this.statisticsCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .recordStats()
                        .expireAfterAccess(icebergConfig.getMetadataCacheTtl().toJavaTime())
                        .maximumSize(icebergConfig.getCacheSize()),
                new CacheLoader<>()
                {
                    @Override
                    public IcebergTableStatistics load(CacheKey key)
                            throws Exception
                    {
                        Table table = key.icebergTable;
                        TableScan scan = table.newScan().useSnapshot(key.snapshotId).includeColumnStats();
                        return new IcebergTableStatistics(table.spec(), typeManager, table.schema().columns(), scan, _ -> true);
                    }
                });
    }

    public TableMetadata getMetadata(String location)
    {
        return metadataCache.getUnchecked(location);
    }

    public IcebergTableStatistics getStatistics(boolean cacheEnabled, Table icebergTable, long snapshotId, TupleDomain<IcebergColumnHandle> filter, TrinoFileSystem fileSystem)
    {
        Domain pathDomain = getPathDomain(filter);
        Domain fileModifiedTimeDomain = getFileModifiedTimePathDomain(filter);

        if (cacheEnabled && pathDomain.isAll() && fileModifiedTimeDomain.isAll() && snapshotId == icebergTable.currentSnapshot().snapshotId()) {
            return statisticsCache.getUnchecked(new CacheKey(icebergTable, snapshotId)).filter(filter);
        }

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(filter))
                .useSnapshot(snapshotId)
                .includeColumnStats();

        return new IcebergTableStatistics(
                icebergTable.spec(),
                typeManager,
                icebergTable.schema().columns(),
                tableScan,
                createFileScanTaskPredicate(fileSystem, pathDomain, fileModifiedTimeDomain));
    }

    private static Predicate<FileScanTask> createFileScanTaskPredicate(TrinoFileSystem fileSystem, Domain pathDomain, Domain fileModifiedTimeDomain)
    {
        return fileScanTask -> {
            if (!pathDomain.isAll() && !pathDomain.includesNullableValue(utf8Slice(fileScanTask.file().path().toString()))) {
                return false;
            }
            if (!fileModifiedTimeDomain.isAll()) {
                long fileModifiedTime = getModificationTime(fileScanTask.file().path().toString(), fileSystem);
                return fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY));
            }
            return true;
        };
    }

    private record CacheKey(Table icebergTable, long snapshotId)
    {
        @Override
        public int hashCode()
        {
            return Objects.hash(icebergTable.location(), snapshotId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return Objects.equals(icebergTable.location(), that.icebergTable.location()) && snapshotId == that.snapshotId;
        }
    }

    @Managed
    public long getMetadataCacheSize()
    {
        return metadataCache.size();
    }

    @Managed
    public Double getMetadataCacheHitRate()
    {
        return metadataCache.stats().hitRate();
    }

    @Managed
    public Double getMetadataCacheMissRate()
    {
        return metadataCache.stats().missRate();
    }

    @Managed
    public long getMetadataCacheRequestCount()
    {
        return metadataCache.stats().requestCount();
    }

    @Managed
    public long getStatisticsCacheSize()
    {
        return statisticsCache.size();
    }

    @Managed
    public Double getStatisticsCacheHitRate()
    {
        return statisticsCache.stats().hitRate();
    }

    @Managed
    public Double getStatisticsCacheMissRate()
    {
        return statisticsCache.stats().missRate();
    }

    @Managed
    public long getStatisticsCacheRequestCount()
    {
        return statisticsCache.stats().requestCount();
    }
}
