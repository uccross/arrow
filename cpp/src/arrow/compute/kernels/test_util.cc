// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/compute/kernels/test_util.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

namespace {

template <typename T>
DatumVector GetDatums(const std::vector<T>& inputs) {
  std::vector<Datum> datums;
  for (const auto& input : inputs) {
    datums.emplace_back(input);
  }
  return datums;
}

void CheckScalarNonRecursive(const std::string& func_name, const DatumVector& inputs,
                             const std::shared_ptr<Array>& expected,
                             const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, inputs, options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

template <typename... SliceArgs>
DatumVector SliceArrays(const DatumVector& inputs, SliceArgs... slice_args) {
  DatumVector sliced;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      sliced.push_back(*input.make_array()->Slice(slice_args...));
    } else {
      sliced.push_back(input);
    }
  }
  return sliced;
}

ScalarVector GetScalars(const DatumVector& inputs, int64_t index) {
  ScalarVector scalars;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      scalars.push_back(*input.make_array()->GetScalar(index));
    } else {
      scalars.push_back(input.scalar());
    }
  }
  return scalars;
}

}  // namespace

void CheckScalar(std::string func_name, const ScalarVector& inputs,
                 std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, GetDatums(inputs), options));
  if (!out.scalar()->Equals(expected)) {
    std::string summary = func_name + "(";
    for (const auto& input : inputs) {
      summary += input->ToString() + ",";
    }
    summary.back() = ')';

    summary += " = " + out.scalar()->ToString() + " != " + expected->ToString();

    if (!out.type()->Equals(expected->type)) {
      summary += " (types differed: " + out.type()->ToString() + " vs " +
                 expected->type->ToString() + ")";
    }

    FAIL() << summary;
  }
}

void CheckScalar(std::string func_name, const DatumVector& inputs,
                 std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalarNonRecursive(func_name, inputs, expected, options);

  // check for at least 1 array, and make sure the others are of equal length
  std::shared_ptr<Array> array;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      if (!array) {
        array = input.make_array();
      } else {
        ASSERT_EQ(input.array()->length, array->length());
      }
    }
  }

  // Check all the input scalars, if scalars are implemented
  if (std::none_of(inputs.begin(), inputs.end(), [](const Datum& datum) {
        return datum.type()->id() == Type::EXTENSION;
      })) {
    // Check all the input scalars
    for (int64_t i = 0; i < array->length(); ++i) {
      CheckScalar(func_name, GetScalars(inputs, i), *expected->GetScalar(i), options);
    }
  }

  // Since it's a scalar function, calling it on sliced inputs should
  // result in the sliced expected output.
  const auto slice_length = array->length() / 3;
  if (slice_length > 0) {
    CheckScalarNonRecursive(func_name, SliceArrays(inputs, 0, slice_length),
                            expected->Slice(0, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceArrays(inputs, slice_length, slice_length),
                            expected->Slice(slice_length, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceArrays(inputs, 2 * slice_length),
                            expected->Slice(2 * slice_length), options);
  }

  // Should also work with an empty slice
  CheckScalarNonRecursive(func_name, SliceArrays(inputs, 0, 0), expected->Slice(0, 0),
                          options);

  // Ditto with ChunkedArray inputs
  if (slice_length > 0) {
    DatumVector chunked_inputs;
    chunked_inputs.reserve(inputs.size());
    for (const auto& input : inputs) {
      if (input.is_array()) {
        auto ar = input.make_array();
        auto ar_chunked = std::make_shared<ChunkedArray>(
            ArrayVector{ar->Slice(0, slice_length), ar->Slice(slice_length)});
        chunked_inputs.push_back(ar_chunked);
      } else {
        chunked_inputs.push_back(input.scalar());
      }
    }
    ArrayVector expected_chunks{expected->Slice(0, slice_length),
                                expected->Slice(slice_length)};

    ASSERT_OK_AND_ASSIGN(Datum out,
                         CallFunction(func_name, GetDatums(chunked_inputs), options));
    ASSERT_OK(out.chunked_array()->ValidateFull());
    AssertDatumsEqual(std::make_shared<ChunkedArray>(expected_chunks), out);
  }
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<Array> input,
                      std::shared_ptr<Array> expected, const FunctionOptions* options) {
  ArrayVector input_vector = {input};
  CheckScalar(std::move(func_name), GetDatums(input_vector), expected, options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  CheckScalarUnary(std::move(func_name), ArrayFromJSON(in_ty, json_input),
                   ArrayFromJSON(out_ty, json_expected), options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<Scalar> input,
                      std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {input}, expected, options);
}

void CheckVectorUnary(std::string func_name, Datum input, std::shared_ptr<Array> expected,
                      const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Scalar> left_input,
                       std::shared_ptr<Scalar> right_input,
                       std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Array> left_input,
                       std::shared_ptr<Array> right_input,
                       std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Array> left_input,
                       std::shared_ptr<Scalar> right_input,
                       std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Scalar> left_input,
                       std::shared_ptr<Array> right_input,
                       std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckDispatchBest(std::string func_name, std::vector<ValueDescr> original_values,
                       std::vector<ValueDescr> expected_equivalent_values) {
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction(func_name));

  auto values = original_values;
  ASSERT_OK_AND_ASSIGN(auto actual_kernel, function->DispatchBest(&values));

  ASSERT_OK_AND_ASSIGN(auto expected_kernel,
                       function->DispatchExact(expected_equivalent_values));

  EXPECT_EQ(actual_kernel, expected_kernel)
      << "  DispatchBest" << ValueDescr::ToString(original_values) << " => "
      << actual_kernel->signature->ToString() << "\n"
      << "  DispatchExact" << ValueDescr::ToString(expected_equivalent_values) << " => "
      << expected_kernel->signature->ToString();
}

void CheckDispatchFails(std::string func_name, std::vector<ValueDescr> values) {
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction(func_name));
  ASSERT_NOT_OK(function->DispatchBest(&values));
  ASSERT_NOT_OK(function->DispatchExact(values));
}

}  // namespace compute
}  // namespace arrow
