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
#include "arrow/dataset/file_rados_parquet.h"

#include "arrow/api.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace dataset {

class RadosParquetScanTask : public ScanTask {
 public:
  RadosParquetScanTask(std::shared_ptr<ScanOptions> options,
                       std::shared_ptr<ScanContext> context, FileSource source,
                       std::shared_ptr<DirectObjectAccess> doa)
      : ScanTask(std::move(options), std::move(context)),
        source_(std::move(source)),
        doa_(std::move(doa)) {}

  Result<RecordBatchIterator> Execute() override {
    ceph::bufferlist* in = new ceph::bufferlist();
    ceph::bufferlist* out = new ceph::bufferlist();

    ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(
        options_->filter, options_->partition_expression, options_->projector.schema(),
        options_->dataset_schema, *in));

    Status s;
    struct stat st;
    s = doa_->Stat(source_.path(), st);
    if (!s.ok()) {
      return Status::Invalid(s.message());
    }

    s = doa_->Exec(st.st_ino, "scan", *in, *out);
    if (!s.ok()) {
      return Status::ExecutionError(s.message());
    }

    RecordBatchVector batches;
    auto buffer = std::make_shared<Buffer>((uint8_t*)out->c_str(), out->length());
    auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
    ARROW_ASSIGN_OR_RAISE(auto rb_reader,
                          arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
    return IteratorFromReader(rb_reader);
  }

 protected:
  FileSource source_;
  std::shared_ptr<DirectObjectAccess> doa_;
};

RadosParquetFileFormat::RadosParquetFileFormat(const std::string& ceph_config_path,
                                               const std::string& data_pool,
                                               const std::string& user_name,
                                               const std::string& cluster_name) {
  auto cluster = std::make_shared<RadosCluster>(ceph_config_path, data_pool, user_name,
                                                cluster_name);
  cluster->Connect();
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>(cluster);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> RadosParquetFileFormat::Inspect(
    const FileSource& source) const {
  ceph::bufferlist in, out;

  Status s = doa_->Exec(source.path(), "read_schema", in, out);
  if (!s.ok()) return Status::ExecutionError(s.message());

  std::vector<std::shared_ptr<Schema>> schemas;
  ipc::DictionaryMemo empty_memo;
  io::BufferReader schema_reader((uint8_t*)out.c_str(), out.length());
  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::ReadSchema(&schema_reader, &empty_memo));
  return schema;
}

Result<ScanTaskIterator> RadosParquetFileFormat::ScanFile(
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context,
    FileFragment* file) const {
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>(*options);
  options_->partition_expression = file->partition_expression();
  options_->dataset_schema = file->dataset_schema();
  ScanTaskVector v{std::make_shared<RadosParquetScanTask>(
      std::move(options_), std::move(context), file->source(), std::move(doa_))};
  return MakeVectorIterator(v);
}

}  // namespace dataset
}  // namespace arrow
