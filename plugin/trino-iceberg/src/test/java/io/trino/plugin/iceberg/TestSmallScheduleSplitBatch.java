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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class TestSmallScheduleSplitBatch
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of(
                        "query.schedule-split-batch-size", "1"))
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    public void testTableChangesWithSmallRowGroup()
    {
        Session smallRowGroupSession = withSmallRowGroups(getSession());
        DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_table_changes_function_",
                "AS SELECT 1 as seq_id, nationkey, name FROM tpch.tiny.nation WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            // write more data to ensure multiple splits are generated
            int copies = 100;
            assertUpdate(smallRowGroupSession, """
                    INSERT INTO %s
                    SELECT i, nationkey, name FROM nation
                    CROSS JOIN UNNEST (sequence(1, %s)) a(i)""".formatted(table.getName(), copies), 25L * copies);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            assertQuery(
                    "SELECT nationkey, name, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal " +
                            "FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s)) where seq_id = %s".formatted(table.getName(), initialSnapshot, snapshotAfterInsert, copies),
                    "SELECT nationkey, name, 'insert', %s, '%s', 0 FROM nation".formatted(snapshotAfterInsert, snapshotAfterInsertTime));
        }
    }

    private long getMostRecentSnapshotId(String tableName)
    {
        return (long) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyColumnAsSet());
    }

    private ZonedDateTime getSnapshotTime(String tableName, long snapshotId)
    {
        return (ZonedDateTime) Iterables.getOnlyElement(getQueryRunner().execute(format("SELECT committed_at FROM \"%s$snapshots\" WHERE snapshot_id = %s", tableName, snapshotId))
                .getOnlyColumnAsSet());
    }
}
