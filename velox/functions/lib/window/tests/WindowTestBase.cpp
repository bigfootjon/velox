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
#include "velox/functions/lib/window/tests/WindowTestBase.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

WindowTestBase::QueryInfo WindowTestBase::buildWindowQuery(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause) {
  std::string functionSql =
      fmt::format("{} over ({} {})", function, overClause, frameClause);
  auto op = PlanBuilder()
                .setParseOptions(options_)
                .values(input)
                .window({functionSql})
                .planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());
  std::string querySql =
      fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql);

  return {op, functionSql, querySql};
}

WindowTestBase::QueryInfo WindowTestBase::buildStreamingWindowQuery(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause) {
  std::string functionSql =
      fmt::format("{} over ({} {})", function, overClause, frameClause);
  std::vector<std::string> orderByClauses;
  auto windowExpr = duckdb::parseWindowExpr(functionSql, {});

  // Extract the partition by keys.
  for (const auto& partition : windowExpr.partitionBy) {
    orderByClauses.push_back(partition->toString() + " NULLS FIRST");
  }

  // Extract the order by keys.
  const auto& orderBy = windowExpr.orderBy;
  for (auto i = 0; i < orderBy.size(); ++i) {
    orderByClauses.push_back(
        orderBy[i].first->toString() + " " + orderBy[i].second.toString());
  }

  // Sort the input data before streaming window.
  auto plan = PlanBuilder()
                  .setParseOptions(options_)
                  .values(input)
                  .orderBy(orderByClauses, false)
                  .streamingWindow({functionSql})
                  .planNode();

  auto rowType = asRowType(input[0]->type());
  std::string columnsString = folly::join(", ", rowType->names());
  std::string querySql =
      fmt::format("SELECT {}, {} FROM tmp", columnsString, functionSql);

  return {plan, functionSql, querySql};
}

RowVectorPtr WindowTestBase::makeSimpleVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row % 7; }, nullEvery(11)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 6 + 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 4 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSinglePartitionVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row; }, nullEvery(7)),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 6 + 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 4 + 1; }),
  });
}

RowVectorPtr WindowTestBase::makeSingleRowPartitionsVector(vector_size_t size) {
  return makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 6 + 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 4 + 1; }),
  });
}

VectorPtr WindowTestBase::makeRandomInputVector(
    const TypePtr& type,
    vector_size_t size,
    float nullRatio) {
  VectorFuzzer::Options options;
  options.vectorSize = size;
  options.nullRatio = nullRatio;
  options.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  VectorFuzzer fuzzer(options, pool_.get(), 0);
  return fuzzer.fuzzFlat(type);
}

RowVectorPtr WindowTestBase::makeRandomInputVector(vector_size_t size) {
  boost::random::mt19937 gen;
  // Frame index values require integer values > 0.
  auto genRandomFrameValue = [&](vector_size_t /*row*/) {
    return boost::random::uniform_int_distribution<int>(1)(gen);
  };
  return makeRowVector(
      {makeRandomInputVector(BIGINT(), size, 0.2),
       makeRandomInputVector(VARCHAR(), size, 0.3),
       makeFlatVector<int64_t>(size, genRandomFrameValue),
       makeFlatVector<int64_t>(size, genRandomFrameValue)});
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::vector<std::string>& overClauses,
    const std::vector<std::string>& frameClauses,
    bool createTable,
    WindowStyle windowStyle) {
  if (createTable) {
    createDuckDbTable(input);
  }

  // Generate a random boolean to determine the window style
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(0, 1);

  int n = 0;
  for (const auto& overClause : overClauses) {
    for (const auto& frameClause : frameClauses) {
      ++n;

      auto resolvedWindowStyle = windowStyle == WindowStyle::kRandom
          ? static_cast<int>(dis(gen))
          : static_cast<int>(windowStyle);

      WindowTestBase::QueryInfo queryInfo;
      if (resolvedWindowStyle == static_cast<int>(WindowStyle::kSort)) {
        queryInfo = buildWindowQuery(input, function, overClause, frameClause);
      } else {
        queryInfo =
            buildStreamingWindowQuery(input, function, overClause, frameClause);
      }

      SCOPED_TRACE(fmt::format("Query #{}: {}", n, queryInfo.functionSql));
      assertQuery(queryInfo.planNode, queryInfo.querySql);
    }
  }
}

void WindowTestBase::testWindowFunction(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause,
    const RowVectorPtr& expectedResult) {
  auto queryInfo = buildWindowQuery(input, function, overClause, frameClause);
  SCOPED_TRACE(queryInfo.functionSql);
  assertQuery(queryInfo.planNode, expectedResult);
}

void WindowTestBase::testKRangeFrames(const std::string& function) {
  vector_size_t size = 20;

  auto rangeFrameTest = [&](const VectorPtr& startColumn,
                            const VectorPtr& endColumn,
                            const std::string& overClause,
                            const std::string& veloxFrame,
                            const std::string& duckFrame) {
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        startColumn,
        endColumn,
    });
    createDuckDbTable({vectors});

    std::string veloxFunction =
        fmt::format("{} over ({} {})", function, overClause, veloxFrame);
    std::string duckFunction =
        fmt::format("{} over ({} {})", function, overClause, duckFrame);
    auto op = PlanBuilder()
                  .setParseOptions(options_)
                  .values({vectors})
                  .window({veloxFunction})
                  .planNode();

    auto rowType = asRowType(vectors->type());
    std::string columnsString = folly::join(", ", rowType->names());
    std::string querySql =
        fmt::format("SELECT {}, {} FROM tmp", columnsString, duckFunction);
    SCOPED_TRACE(veloxFunction);
    assertQuery(op, querySql);
  };

  // For frames with k RANGE PRECEDING/FOLLOWING, Velox requires the application
  // to add columns with the range frame boundary value computed according
  // to the frame type.
  // If the frame is k PRECEDING :
  // frame_boundary_value = current_order_by - k (for ascending ORDER BY)
  // frame_boundary_value = current_order_by + k (for descending ORDER BY)
  // If the frame is k FOLLOWING :
  // frame_boundary_value = current_order_by + k (for ascending ORDER BY)
  // frame_boundary_value = current_order_by - k (for descending ORDER BY)

  // The test-cases below cover different variations of k PRECEDING/k FOLLOWING
  // with ascending and descending ORDER BY.
  std::string overClause = "partition by c0 order by c1";
  std::string descOverClause = "partition by c0 order by c1 desc";

  // Computed frame boundary columns for asc/desc ORDER BY.
  auto startColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row - 4; });
  auto descStartColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row + 4; });
  auto endColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row + 2; });
  auto descEndColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row - 2; });

  // The Velox window function requires a frame column whereas DuckDB can use
  // the SQL window frame it represents.
  std::string veloxFrame = "range between c2 preceding and c3 following";
  std::string duckFrame = "range between 4 preceding and 2 following";
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  duckFrame = "range between 4 preceding and current row";
  veloxFrame = "range between c2 preceding and current row";
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  // There are no rows between 2 preceding and 4 preceding frames. So this tests
  // empty frames.
  duckFrame = "range between 4 preceding and 2 preceding";
  veloxFrame = "range between c2 preceding and c3 preceding";
  endColumn = makeFlatVector<int64_t>(size, [](auto row) { return row - 2; });
  descEndColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row + 2; });
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  // There is exactly one row in the frames between 2 and 6 preceding values.
  startColumn = makeFlatVector<int64_t>(size, [](auto row) { return row - 6; });
  descStartColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row + 6; });
  duckFrame = "range between 6 preceding and 2 preceding";
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  // There are no rows between 2 and 4 following frames. So this tests empty
  // frames.
  duckFrame = "range between 2 following and 4 following";
  veloxFrame = "range between c2 following and c3 following";
  startColumn = makeFlatVector<int64_t>(size, [](auto row) { return row + 2; });
  descStartColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row - 2; });
  endColumn = makeFlatVector<int64_t>(size, [](auto row) { return row + 4; });
  descEndColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row - 4; });
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  duckFrame = "range between current row and 4 following";
  veloxFrame = "range between current row and c3 following";
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);

  // There is exactly one row between 2 and 6 following frame values.
  duckFrame = "range between 2 following and 6 following";
  veloxFrame = "range between c2 following and c3 following";
  endColumn = makeFlatVector<int64_t>(size, [](auto row) { return row + 6; });
  descEndColumn =
      makeFlatVector<int64_t>(size, [](auto row) { return row - 6; });
  rangeFrameTest(startColumn, endColumn, overClause, veloxFrame, duckFrame);
  rangeFrameTest(
      descStartColumn, descEndColumn, descOverClause, veloxFrame, duckFrame);
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& errorMessage) {
  assertWindowFunctionError(input, function, overClause, "", errorMessage);
}

void WindowTestBase::assertWindowFunctionError(
    const std::vector<RowVectorPtr>& input,
    const std::string& function,
    const std::string& overClause,
    const std::string& frameClause,
    const std::string& errorMessage) {
  auto queryInfo = buildWindowQuery(input, function, overClause, frameClause);
  SCOPED_TRACE(queryInfo.functionSql);

  VELOX_ASSERT_THROW(
      assertQuery(queryInfo.planNode, queryInfo.querySql), errorMessage);
}

}; // namespace facebook::velox::window::test
