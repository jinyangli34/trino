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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;

public class IcebergTableStatistics
{
    private final PartitionSpec partitionSpec;
    private final List<Types.NestedField> columns;
    private final Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionStatistics;
    private IcebergStatistics tableStatistics;

    public IcebergTableStatistics(
            PartitionSpec partitionSpec,
            TypeManager typeManager,
            List<Types.NestedField> columns,
            TableScan tableScan,
            Predicate<FileScanTask> filePredicate)
    {
        this(partitionSpec, columns, getStatisticsByPartition(tableScan, columns, typeManager, filePredicate));
    }

    public IcebergTableStatistics(PartitionSpec partitionSpec, List<Types.NestedField> columns, Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionStatistics)
    {
        this.partitionSpec = partitionSpec;
        this.columns = columns;
        this.partitionStatistics = partitionStatistics;
    }

    public IcebergStatistics getTableStatistics(TypeManager typeManager)
    {
        if (tableStatistics == null) {
            IcebergStatistics.Builder builder = new IcebergStatistics.Builder(columns, typeManager);
            (partitionStatistics.size() > 10000 ? partitionStatistics.values().parallelStream() : partitionStatistics.values().stream())
                    .forEach(builder::acceptStatistics);
            tableStatistics = builder.build();
        }
        return tableStatistics;
    }

    public Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getPartitionStatistics()
    {
        return partitionStatistics;
    }

    public IcebergTableStatistics filter(TupleDomain<IcebergColumnHandle> filter)
    {
        if (filter.isNone()) {
            return new IcebergTableStatistics(partitionSpec, columns, ImmutableMap.of());
        }
        if (filter.isAll() || filter.getDomains().isEmpty()) {
            return this;
        }

        Map<IcebergColumnHandle, Domain> domains = filter.getDomains().get();

        ImmutableMap.Builder<Integer, Optional<Integer>> builder = ImmutableMap.builder();
        for (IcebergColumnHandle columnHandle : domains.keySet()) {
            Optional<Integer> partitionFieldId = partitionSpec.getFieldsBySourceId(columnHandle.getId()).stream()
                    .filter(field -> field.transform().isIdentity())
                    .map(PartitionField::fieldId)
                    .findAny();
            builder.put(columnHandle.getId(), partitionFieldId);
        }
        Map<Integer, Optional<Integer>> columnIdToFieldId = builder.buildOrThrow();

        Map<ColumnValue, Boolean> cache = new HashMap<>();
        Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> filtered = partitionStatistics.entrySet().stream()
                .filter(e -> filterPartition(cache, domains, columnIdToFieldId, e.getKey(), e.getValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        return new IcebergTableStatistics(partitionSpec, columns, filtered);
    }

    private boolean filterStats(IcebergStatistics statistics, IcebergColumnHandle columnHandle, int colId, Domain domain)
    {
        Object minValue = statistics.minValues().get(colId);
        Object maxValue = statistics.maxValues().get(colId);
        Domain statisticsDomain = Domain.create(
                ValueSet.ofRanges(Range.range(columnHandle.getType(), minValue, true, maxValue, true)),
                statistics.nullCounts().get(colId) > 0);
        return !statisticsDomain.intersect(domain).isNone();
    }

    private boolean filterPartition(Map<ColumnValue, Boolean> cache, Map<IcebergColumnHandle, Domain> domains, Map<Integer, Optional<Integer>> columnIdToFieldId, StructLikeWrapperWithFieldIdToIndex key, IcebergStatistics statistics)
    {
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domains.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            if (domain == null) {
                continue;
            }

            Optional<Integer> partitionFieldId = columnIdToFieldId.get(columnHandle.getId());
            if (partitionFieldId.isEmpty() || !key.fieldIdToIndex().containsKey(partitionFieldId.get())) {
                if (!filterStats(statistics, columnHandle, columnHandle.getId(), domain)) {
                    return false;
                }
            }
            else {
                Class<?> javaClass = toIcebergType(columnHandle.getType(), columnHandle.getColumnIdentity()).typeId().javaClass();
                int pos = key.fieldIdToIndex().get(partitionFieldId.get());
                ColumnValue columnValue = new ColumnValue(columnHandle.getId(), key.structLikeWrapper().get().get(pos, javaClass));
                boolean match = cache.computeIfAbsent(columnValue, cv -> {
                    Object castValue = cv.value();
                    if (cv.value() != null) {
                        if (columnHandle.getType().getJavaType().equals(Slice.class) && cv.value() instanceof String valueString) {
                            castValue = Slices.utf8Slice(valueString);
                        }
                        else if (javaClass.equals(Integer.class)) {
                            castValue = ((Integer) cv.value()).longValue();
                        }
                    }

                    return domain.includesNullableValue(castValue);
                });
                if (!match) {
                    return false;
                }
            }
        }

        return true;
    }

    private static Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getStatisticsByPartition(
            TableScan tableScan, List<Types.NestedField> columns, TypeManager typeManager, Predicate<FileScanTask> predicate)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics.Builder> partitions = new HashMap<>();
            for (FileScanTask fileScanTask : fileScanTasks) {
                if (!predicate.test(fileScanTask)) {
                    continue;
                }
                DataFile dataFile = fileScanTask.file();
                Types.StructType structType = fileScanTask.spec().partitionType();
                StructLike partitionStruct = dataFile.partition();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(partitionStruct);
                StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = new StructLikeWrapperWithFieldIdToIndex(partitionWrapper, structType);

                partitions.computeIfAbsent(
                                structLikeWrapperWithFieldIdToIndex,
                                _ -> new IcebergStatistics.Builder(columns, typeManager))
                        .acceptDataFile(dataFile, fileScanTask.spec());
            }

            return partitions.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record ColumnValue(
            int columnId,
            Object value)
    {
    }
}
