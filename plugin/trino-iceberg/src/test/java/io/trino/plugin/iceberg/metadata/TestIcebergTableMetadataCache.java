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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.TableStatisticsReader;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableMetadataCache
        extends AbstractTestQueryFramework
{
    private final TestingConnectorSession session = TestingConnectorSession.builder()
            .setPropertyMetadata(ImmutableList.of(
                    PropertyMetadata.booleanProperty("extended_statistics_enabled", "extended_statistics_enabled", true, false),
                    PropertyMetadata.booleanProperty("metadata_cache_enabled", "metadata_cache_enabled", true, false)))
            .build();

    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .build();

        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data")));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.security", "allow-all")
                .buildOrThrow());

        try {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
        }
        catch (RuntimeException e) {
            Closables.closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystemFactory()
    {
        fileSystemFactory = getFileSystemFactory(getDistributedQueryRunner());
    }

    @Test
    public void testStatsFilePruningWithCache()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_stats_file_pruning_", "(a INT, b INT) WITH (partitioning = ARRAY['b'])")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, 10), (10, 10)", 2);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (200, 10), (300, 20)", 2);

            Optional<Long> snapshotId = Optional.of((long) computeScalar("SELECT snapshot_id FROM \"" + testTable.getName() + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"));
            TypeManager typeManager = new TestingTypeManager();

            Table table = loadTable(testTable.getName());
            IcebergColumnHandle columnA = IcebergUtil.getColumnHandle(table.schema().findField("a"), typeManager);
            IcebergColumnHandle columnB = IcebergUtil.getColumnHandle(table.schema().findField("b"), typeManager);

            IcebergTableMetadataCache metadataCache = new IcebergTableMetadataCache(fileSystemFactory, new IcebergConfig(), typeManager);
            TableStatistics withNoFilter = TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, TupleDomain.all(), TupleDomain.all(), fileSystemFactory.create(session), metadataCache);
            assertThat(withNoFilter.getRowCount().getValue()).isEqualTo(4.0);

            TableStatistics withPartitionFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    session,
                    table,
                    snapshotId,
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            columnB,
                            Domain.singleValue(INTEGER, 10L))),
                    TupleDomain.all(),
                    fileSystemFactory.create(session),
                    metadataCache);
            assertThat(withPartitionFilter.getRowCount().getValue()).isEqualTo(3.0);

            TableStatistics withUnenforcedFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    session,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            columnA,
                            Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 250L)), true))),
                    fileSystemFactory.create(SESSION),
                    metadataCache);
            assertThat(withUnenforcedFilter.getRowCount().getValue()).isEqualTo(1.0);
        }
    }

    @Test
    public void testStatsFilePruningWithoutCache()
    {
        TestingConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(ImmutableList.of(
                        PropertyMetadata.booleanProperty("extended_statistics_enabled", "extended_statistics_enabled", true, false),
                        PropertyMetadata.booleanProperty("metadata_cache_enabled", "metadata_cache_enabled", false, false)))
                .build();

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_stats_file_pruning_", "(a INT, b INT) WITH (partitioning = ARRAY['b'])")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, 10), (10, 10)", 2);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (200, 10), (300, 20)", 2);

            Optional<Long> snapshotId = Optional.of((long) computeScalar("SELECT snapshot_id FROM \"" + testTable.getName() + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"));
            TypeManager typeManager = new TestingTypeManager();

            Table table = loadTable(testTable.getName());
            IcebergColumnHandle columnA = IcebergUtil.getColumnHandle(table.schema().findField("a"), typeManager);
            IcebergColumnHandle columnB = IcebergUtil.getColumnHandle(table.schema().findField("b"), typeManager);

            IcebergTableMetadataCache metadataCache = new IcebergTableMetadataCache(fileSystemFactory, new IcebergConfig(), typeManager);
            TableStatistics withNoFilter = TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, TupleDomain.all(), TupleDomain.all(), fileSystemFactory.create(session), metadataCache);
            assertThat(withNoFilter.getRowCount().getValue()).isEqualTo(4.0);

            TableStatistics withPartitionFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    session,
                    table,
                    snapshotId,
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            columnB,
                            Domain.singleValue(INTEGER, 10L))),
                    TupleDomain.all(),
                    fileSystemFactory.create(session),
                    metadataCache);
            assertThat(withPartitionFilter.getRowCount().getValue()).isEqualTo(3.0);

            TableStatistics withUnenforcedFilter = TableStatisticsReader.makeTableStatistics(
                    typeManager,
                    session,
                    table,
                    snapshotId,
                    TupleDomain.all(),
                    TupleDomain.withColumnDomains(ImmutableMap.of(
                            columnA,
                            Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 100L)), true))),
                    fileSystemFactory.create(SESSION),
                    metadataCache);
            assertThat(withUnenforcedFilter.getRowCount().getValue()).isEqualTo(2.0);
        }
    }

    @Test
    public void testStatisticsCache()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute, "test_statistics_cache_", "(a INT, b INT) WITH (partitioning = ARRAY['b'])")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (1, 10), (10, 10)", 2);
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (200, 10), (300, 20)", 2);

            Optional<Long> snapshotId = Optional.of((long) computeScalar("SELECT snapshot_id FROM \"" + testTable.getName() + "$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES"));
            TypeManager typeManager = new TestingTypeManager();
            Table table = loadTable(testTable.getName());
            IcebergConfig icebergConfig = new IcebergConfig().setMetadataCacheEnabled(true);
            IcebergTableMetadataCache metadataCache = new IcebergTableMetadataCache(fileSystemFactory, icebergConfig, typeManager);
            IcebergColumnHandle columnA = IcebergUtil.getColumnHandle(table.schema().findField("a"), typeManager);
            IcebergColumnHandle columnB = IcebergUtil.getColumnHandle(table.schema().findField("b"), typeManager);

            TupleDomain<IcebergColumnHandle> filter1 = TupleDomain.withColumnDomains(
                    ImmutableMap.of(columnB, Domain.singleValue(INTEGER, 10L)));
            TupleDomain<IcebergColumnHandle> filter2 = TupleDomain.withColumnDomains(
                    ImmutableMap.of(columnA, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 100L)), true)));

            validateCache(metadataCache, 0, 0, 1.0, 0.0);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, TupleDomain.all(), TupleDomain.all(), fileSystem, metadataCache);
            validateCache(metadataCache, 1, 1, 0.0, 1.0);

            TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, filter1, TupleDomain.all(), fileSystem, metadataCache);
            validateCache(metadataCache, 1, 2, 0.5, 0.5);

            TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, TupleDomain.all(), filter2, fileSystem, metadataCache);
            validateCache(metadataCache, 1, 3, 2.0 / 3, 1.0 / 3);

            TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, TupleDomain.all(), TupleDomain.all(), fileSystem, metadataCache);
            validateCache(metadataCache, 1, 4, 0.75, 0.25);

            TableStatisticsReader.makeTableStatistics(typeManager, session, table, snapshotId, filter1, filter2, fileSystem, metadataCache);
            validateCache(metadataCache, 1, 5, 0.8, 0.2);
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }

    private static void validateCache(IcebergTableMetadataCache metadataCache, int size, int requestCount, double hitRate, double missRate)
    {
        assertThat(metadataCache.getStatisticsCacheSize()).isEqualTo(size);
        assertThat(metadataCache.getStatisticsCacheRequestCount()).isEqualTo(requestCount);
        Assertions.assertEquals(hitRate, metadataCache.getStatisticsCacheHitRate());
        Assertions.assertEquals(missRate, metadataCache.getStatisticsCacheMissRate());
    }
}
