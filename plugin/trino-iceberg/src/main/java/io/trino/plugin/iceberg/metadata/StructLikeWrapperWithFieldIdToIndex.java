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

import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public record StructLikeWrapperWithFieldIdToIndex(
        StructLikeWrapper structLikeWrapper,
        Map<Integer, Integer> fieldIdToIndex)
{
    public StructLikeWrapperWithFieldIdToIndex(StructLikeWrapper structLikeWrapper, Types.StructType structType)
    {
        this(structLikeWrapper,
                IntStream.range(0, structType.fields().size()).boxed()
                        .collect(toImmutableMap(i -> structType.fields().get(i).fieldId(), i -> i)));
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
        StructLikeWrapperWithFieldIdToIndex that = (StructLikeWrapperWithFieldIdToIndex) o;
        // Due to bogus implementation of equals in StructLikeWrapper https://github.com/apache/iceberg/issues/5064 order here matters.
        return Objects.equals(fieldIdToIndex, that.fieldIdToIndex) && Objects.equals(structLikeWrapper, that.structLikeWrapper);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldIdToIndex, structLikeWrapper);
    }
}
