/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#pragma once

#include <folly/container/F14Set.h>
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/Strings.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace detail {

/// Maintains a set of unique values. Non-null values are stored in F14FastSet.
/// A separate flag tracks presence of the null value.
template <
    typename T,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
struct SetAccumulator {
  bool hasNull{false};
  folly::F14FastSet<T, Hash, EqualTo, AlignedStlAllocator<T, 16>> uniqueValues;

  SetAccumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : uniqueValues{AlignedStlAllocator<T, 16>(allocator)} {}

  SetAccumulator(Hash hash, EqualTo equalTo, HashStringAllocator* allocator)
      : uniqueValues{0, hash, equalTo, AlignedStlAllocator<T, 16>(allocator)} {}

  /// Adds value if new. No-op if the value was added before.
  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    if (decoded.isNullAt(index)) {
      hasNull = true;
    } else {
      uniqueValues.insert(decoded.valueAt<T>(index));
    }
  }

  /// Adds new values from an array.
  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  void addSerialized(
      const FlatVector<StringView>& vector,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    VELOX_CHECK(!vector.isNullAt(index));
    auto serialized = vector.valueAt(index);
    memcpy(&hasNull, serialized.data(), 1);

    T value;
    auto valueSize = sizeof(T);
    auto offset = 1;
    while (offset < serialized.size()) {
      memcpy(&value, serialized.data() + offset, valueSize);
      uniqueValues.insert(value);
      offset += valueSize;
    }
  }

  /// Returns number of unique values including null.
  size_t size() const {
    return uniqueValues.size() + (hasNull ? 1 : 0);
  }

  /// Copies the unique values and null into the specified vector starting at
  /// the specified offset.
  vector_size_t extractValues(FlatVector<T>& values, vector_size_t offset) {
    vector_size_t index = offset;
    for (auto value : uniqueValues) {
      values.set(index++, value);
    }

    if (hasNull) {
      values.setNull(index++, true);
    }

    return index - offset;
  }

  /// Extracts in result[index] a serialized VARBINARY for the Set Values.
  /// This is used for the spill of this accumulator.
  void extractSerialized(const VectorPtr& result, vector_size_t index) {
    // The serialized value is a single byte for hasNull followed by
    // all the values.
    size_t valueSize = sizeof(T);
    size_t totalBytes = valueSize * uniqueValues.size() + 1;

    auto* flatResult = result->as<FlatVector<StringView>>();
    auto* rawBuffer = flatResult->getRawStringBufferWithSpace(totalBytes, true);
    memcpy(rawBuffer, &hasNull, 1);
    size_t offset = 1;
    for (const auto& value : uniqueValues) {
      memcpy(rawBuffer + offset, &value, valueSize);
      offset += valueSize;
    }
    flatResult->setNoCopy(index, StringView(rawBuffer, totalBytes));
    return;
  }

  void free(HashStringAllocator& allocator) {
    using UT = decltype(uniqueValues);
    uniqueValues.~UT();
  }
};

/// Maintains a set of unique strings.
struct StringViewSetAccumulator {
  /// A set of unique StringViews pointing to storage managed by 'strings'.
  SetAccumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  /// Size (in bytes) of the string values. Used for computing serialized
  /// buffer size for spilling.
  size_t stringSetBytes = 0;

  StringViewSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{type, allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    if (decoded.isNullAt(index)) {
      base.hasNull = true;
    } else {
      auto value = decoded.valueAt<StringView>(index);
      addValue(value, allocator);
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  void addSerialized(
      const FlatVector<StringView>& vector,
      vector_size_t index,
      HashStringAllocator* allocator) {
    VELOX_CHECK(!vector.isNullAt(index));
    auto serialized = vector.valueAt(index);
    memcpy(&base.hasNull, serialized.data(), 1);

    auto offset = 1;
    size_t length;
    while (offset < serialized.size()) {
      memcpy(&length, serialized.data() + offset, 4);
      offset += 4;

      StringView value = StringView(serialized.data() + offset, length);
      addValue(value, allocator);
      offset += length;
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(
      FlatVector<StringView>& values,
      vector_size_t offset) {
    return base.extractValues(values, offset);
  }

  /// Extracts in result[index] a serialized VARBINARY for the String Values.
  /// This is used for the spill of this accumulator.
  void extractSerialized(const VectorPtr& result, vector_size_t index) {
    // hasNull is serialized followed by all the String values.
    // Each string value has 4 bytes for its size followed by the string value.
    size_t totalBytes = 1 + stringSetBytes + 4 * (base.uniqueValues.size());

    auto* flatResult = result->as<FlatVector<StringView>>();
    auto* rawBuffer = flatResult->getRawStringBufferWithSpace(totalBytes, true);

    memcpy(rawBuffer, &base.hasNull, 1);
    size_t offset = 1;
    for (const auto& value : base.uniqueValues) {
      const auto valueSize = value.size();
      memcpy(rawBuffer + offset, &valueSize, 4);
      offset += 4;
      memcpy(rawBuffer + offset, value.data(), valueSize);
      offset += valueSize;
    }

    flatResult->setNoCopy(index, StringView(rawBuffer, totalBytes));
    return;
  }

  void free(HashStringAllocator& allocator) {
    strings.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }

 private:
  void addValue(const StringView& value, HashStringAllocator* allocator) {
    if (base.uniqueValues.contains(value)) {
      return;
    }
    StringView insertValue = value;
    if (!insertValue.isInline()) {
      insertValue = strings.append(value, *allocator);
    }
    base.uniqueValues.insert(insertValue);
    stringSetBytes += insertValue.size();
  }
};

/// Maintains a set of unique arrays, maps or structs.
struct ComplexTypeSetAccumulator {
  /// A set of pointers to values stored in AddressableNonNullValueList.
  SetAccumulator<
      HashStringAllocator::Position,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  /// Stores unique non-null values.
  AddressableNonNullValueList values;

  /// Tracks allocated bytes for sizing during serialization for spill.
  size_t valuesSize = 0;

  std::unordered_map<
      HashStringAllocator::Position,
      size_t,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::HashEqualTo>
      valuesLengths;

  ComplexTypeSetAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  void addValue(
      const DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    if (decoded.isNullAt(index)) {
      base.hasNull = true;
    } else {
      auto startSize = allocator->cumulativeBytes();
      auto position = values.append(decoded, index, allocator);

      if (!base.uniqueValues.insert(position).second) {
        values.removeLast(position);
      }

      valuesSize += allocator->cumulativeBytes() - startSize;
      valuesLengths.insert({position, valuesSize});
    }
  }

  void addValues(
      const ArrayVector& arrayVector,
      vector_size_t index,
      const DecodedVector& values,
      HashStringAllocator* allocator) {
    const auto size = arrayVector.sizeAt(index);
    const auto offset = arrayVector.offsetAt(index);

    for (auto i = 0; i < size; ++i) {
      addValue(values, offset + i, allocator);
    }
  }

  void addSerialized(
      const FlatVector<StringView>& vector,
      vector_size_t index,
      HashStringAllocator* allocator) {
    VELOX_CHECK(!vector.isNullAt(index));
    auto serialized = vector.valueAt(index);
    memcpy(&base.hasNull, serialized.data(), 1);

    auto offset = 1;
    size_t length;
    while (offset < serialized.size()) {
      memcpy(&length, serialized.data() + offset, 4);
      offset += 4;

      StringView value = StringView(serialized.data() + offset, length);
      auto startSize = allocator->cumulativeBytes();
      auto position = values.appendSerialized(value, allocator);

      if (!base.uniqueValues.insert(position).second) {
        values.removeLast(position);
      }

      valuesSize += allocator->cumulativeBytes() - startSize;
      valuesLengths.insert({position, valuesSize});

      offset += length;
    }
  }

  size_t size() const {
    return base.size();
  }

  vector_size_t extractValues(BaseVector& values, vector_size_t offset) {
    vector_size_t index = offset;
    for (const auto& position : base.uniqueValues) {
      AddressableNonNullValueList::read(position, values, index++);
    }

    if (base.hasNull) {
      values.setNull(index++, true);
    }

    return index - offset;
  }

  /// Extracts in result[index] a serialized VARBINARY for the String Values.
  /// This is used for the spill of this accumulator.
  void extractSerialized(const VectorPtr& result, vector_size_t index) {
    // hasNull is serialized followed by all the ComplexType values. Each value
    // has 4 bytes for the size + actual value.
    size_t totalBytes = 1 + 4 * size() + valuesSize;

    auto* flatResult = result->as<FlatVector<StringView>>();
    auto* rawBuffer = flatResult->getRawStringBufferWithSpace(totalBytes, true);

    memcpy(rawBuffer, &base.hasNull, 1);
    size_t offset = 1;
    for (const auto& value : base.uniqueValues) {
      VELOX_CHECK(valuesLengths.count(value) != 0);
      auto length = valuesLengths[value];
      memcpy(rawBuffer + offset, &length, 4);
      offset += 4;
      AddressableNonNullValueList::copy(value, rawBuffer + offset, length);
      offset += length;
    }

    flatResult->setNoCopy(index, StringView(rawBuffer, totalBytes));
    return;
  }

  void free(HashStringAllocator& allocator) {
    values.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }
};

template <typename T>
struct SetAccumulatorTypeTraits {
  using AccumulatorType = SetAccumulator<T>;
};

template <>
struct SetAccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewSetAccumulator;
};

template <>
struct SetAccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeSetAccumulator;
};
} // namespace detail

template <typename T>
using SetAccumulator =
    typename detail::SetAccumulatorTypeTraits<T>::AccumulatorType;

} // namespace facebook::velox::aggregate::prestosql
