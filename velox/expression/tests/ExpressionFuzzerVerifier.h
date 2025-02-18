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

#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/expression/tests/utils/FuzzerToolkit.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_int32(velox_fuzzer_max_level_of_nesting);

namespace facebook::velox::test {

// A tool utilizes ExpressionFuzzer, VectorFuzzer and ExpressionVerfier to
// generate random expressions and verify the correctness of the results.
class ExpressionFuzzerVerifier {
 public:
  ExpressionFuzzerVerifier(
      const FunctionSignatureMap& signatureMap,
      size_t initialSeed);

  // This function starts the test that is performed by the
  // ExpressionFuzzerVerifier which is generating random expressions and
  // verifying them.
  void go();

 private:
  struct ExprUsageStats {
    // Num of times the expression was randomly selected.
    int numTimesSelected = 0;
    // Num of rows processed by the expression.
    int numProcessedRows = 0;
  };

  void seed(size_t seed);

  void reSeed();

  // A utility class used to keep track of stats relevant to the fuzzer.
  class ExprStatsListener : public exec::ExprSetListener {
   public:
    explicit ExprStatsListener(
        std::unordered_map<std::string, ExprUsageStats>& exprNameToStats)
        : exprNameToStats_(exprNameToStats) {}

    void onCompletion(
        const std::string& /*uuid*/,
        const exec::ExprSetCompletionEvent& event) override {
      for (auto& [funcName, stats] : event.stats) {
        auto itr = exprNameToStats_.find(funcName);
        if (itr == exprNameToStats_.end()) {
          // Skip expressions like FieldReference and ConstantExpr
          continue;
        }
        itr->second.numProcessedRows += stats.numProcessedRows;
      }
    }

    // A no-op since we cannot tie errors directly to functions where they
    // occurred.
    void onError(
        const SelectivityVector& /*rows*/,
        const ::facebook::velox::ErrorVector& /*errors*/,
        const std::string& /*queryId*/) override {}

   private:
    std::unordered_map<std::string, ExprUsageStats>& exprNameToStats_;
  };

  /// Randomize initial result vector data to test for correct null and data
  /// setting in functions.
  RowVectorPtr generateResultVectors(std::vector<core::TypedExprPtr>& plans);

  /// Executes two steps:
  /// #1. Retries executing the expression in `plan` by wrapping it in a `try()`
  ///     clause and expecting it not to throw an exception.
  /// #2. Re-execute the expression only on rows that produced non-NULL values
  ///     in the previous step.
  ///
  /// Throws in case any of these steps fail.
  void retryWithTry(
      std::vector<core::TypedExprPtr> plans,
      const RowVectorPtr& rowVector,
      const VectorPtr& resultVectors,
      const std::vector<int>& columnsToWrapInLazy);

  /// If --duration_sec > 0, check if we expired the time budget. Otherwise,
  /// check if we expired the number of iterations (--steps).
  template <typename T>
  bool isDone(size_t i, T startTime) const;

  /// Called at the end of a successful fuzzer run. It logs the top and bottom
  /// 10 functions based on the num of rows processed by them. Also logs a full
  /// list of all functions sorted in descending order by the num of times they
  /// were selected by the fuzzer. Every logged function contains the
  /// information in the following format which can be easily exported to a
  /// spreadsheet for further analysis: functionName numTimesSelected
  /// proportionOfTimesSelected numProcessedRows.
  void logStats();

  FuzzerGenerator rng_;

  size_t currentSeed_{0};

  std::shared_ptr<core::QueryCtx> queryCtx_{std::make_shared<core::QueryCtx>()};

  std::shared_ptr<memory::MemoryPool> pool_{memory::addDefaultLeafMemoryPool()};

  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};

  test::ExpressionVerifier verifier_;

  std::shared_ptr<VectorFuzzer> vectorFuzzer_;

  std::shared_ptr<ExprStatsListener> statListener_;

  std::unordered_map<std::string, ExprUsageStats> exprNameToStats_;

  /// The expression fuzzer that is used to fuzz the expression in the test.
  ExpressionFuzzer expressionFuzzer_;
};

} // namespace facebook::velox::test
