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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/expression/tests/ExpressionFuzzerVerifier.h"
#include "velox/functions/FunctionRegistry.h"

/// FuzzerRunner leverages ExpressionFuzzer and VectorFuzzer to automatically
/// generate and execute expression tests. It works by:
///
///  1. Taking an initial set of available function signatures.
///  2. Generating a random expression tree based on the available function
///     signatures.
///  3. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  4. Executing the expression using the common and simplified eval paths, and
///     asserting results are the exact same.
///  5. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./velox_expression_fuzzer_test --steps 10000
///
/// The important flags that control Fuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --only: restrict the functions to fuzz.
///  --batch_size: size of input vector batches generated.
///
/// e.g:
///
///  $ ./velox_expression_fuzzer_test \
///         --steps 10000 \
///         --seed 123 \
///         --v=1 \
///         --only "substr,trim"

class FuzzerRunner {
  static std::unordered_set<std::string> splitNames(const std::string& names) {
    // Parse, lower case and trim it.
    std::vector<folly::StringPiece> nameList;
    folly::split(',', names, nameList);
    std::unordered_set<std::string> nameSet;

    for (const auto& it : nameList) {
      auto str = folly::trimWhitespace(it).toString();
      folly::toLowerAscii(str);
      nameSet.insert(str);
    }
    return nameSet;
  }

  static std::pair<std::string, std::string> splitSignature(
      const std::string& signature) {
    const auto parenPos = signature.find("(");

    if (parenPos != std::string::npos) {
      return {signature.substr(0, parenPos), signature.substr(parenPos)};
    }

    return {signature, ""};
  }

  // Parse the comma separated list of function names, and use it to filter the
  // input signatures.
  static facebook::velox::FunctionSignatureMap filterSignatures(
      const facebook::velox::FunctionSignatureMap& input,
      const std::string& onlyFunctions,
      const std::unordered_set<std::string>& skipFunctions) {
    if (onlyFunctions.empty()) {
      if (skipFunctions.empty()) {
        return input;
      }
      facebook::velox::FunctionSignatureMap output(input);
      for (auto skip : skipFunctions) {
        // 'skip' can be function name or signature.
        const auto [skipName, skipSignature] = splitSignature(skip);

        if (skipSignature.empty()) {
          output.erase(skipName);
        } else {
          auto it = output.find(skipName);
          if (it != output.end()) {
            // Compiler refuses to reference 'skipSignature' from the lambda as
            // is.
            const auto& signatureToRemove = skipSignature;

            auto removeIt = std::find_if(
                it->second.begin(),
                it->second.end(),
                [&](const auto& signature) {
                  return signature->toString() == signatureToRemove;
                });
            VELOX_CHECK(
                removeIt != it->second.end(),
                "Skip signature not found: {}",
                skip);
            it->second.erase(removeIt);
          }
        }
      }
      return output;
    }

    // Parse, lower case and trim it.
    auto nameSet = splitNames(onlyFunctions);

    // Use the generated set to filter the input signatures.
    facebook::velox::FunctionSignatureMap output;
    for (const auto& it : input) {
      if (nameSet.count(it.first) > 0) {
        output.insert(it);
      }
    }
    return output;
  }

  static const std::unordered_map<
      std::string,
      std::vector<facebook::velox::exec::FunctionSignaturePtr>>
      kSpecialForms;

  static void appendSpecialForms(
      const std::string& specialForms,
      facebook::velox::FunctionSignatureMap& signatureMap) {
    auto specialFormNames = splitNames(specialForms);
    for (const auto& [name, signatures] : kSpecialForms) {
      if (specialFormNames.count(name) == 0) {
        LOG(INFO) << "Skipping special form: " << name;
        continue;
      }
      std::vector<const facebook::velox::exec::FunctionSignature*>
          rawSignatures;
      for (const auto& signature : signatures) {
        rawSignatures.push_back(signature.get());
      }
      signatureMap.insert({name, std::move(rawSignatures)});
    }
  }

 public:
  static int run(
      const std::string& onlyFunctions,
      size_t seed,
      const std::unordered_set<std::string>& skipFunctions,
      const std::string& specialForms) {
    runFromGtest(onlyFunctions, seed, skipFunctions, specialForms);
    return RUN_ALL_TESTS();
  }

  static void runFromGtest(
      const std::string& onlyFunctions,
      size_t seed,
      const std::unordered_set<std::string>& skipFunctions,
      const std::string& specialForms) {
    auto signatures = facebook::velox::getFunctionSignatures();
    appendSpecialForms(specialForms, signatures);
    facebook::velox::test::ExpressionFuzzerVerifier(
        filterSignatures(signatures, onlyFunctions, skipFunctions), seed)
        .go();
  }
};
