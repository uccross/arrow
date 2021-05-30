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
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

#include <flatbuffers/flatbuffers.h>

#include "arrow/util/compression.h"
#include "generated/Request_generated.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace dataset {

class RadosParquetScanTask : public ScanTask {
 public:
  RadosParquetScanTask(std::shared_ptr<ScanOptions> options,
                       std::shared_ptr<Fragment> fragment, FileSource source,
                       std::shared_ptr<DirectObjectAccess> doa)
      : ScanTask(std::move(options), std::move(fragment)),
        source_(std::move(source)),
        doa_(std::move(doa)) {}

  Result<RecordBatchIterator> Execute() override {
    Status s;
    struct stat st {};
    s = doa_->Stat(source_.path(), st);
    if (!s.ok()) {
      return Status::Invalid(s.message());
    }

    ceph::bufferlist in;
    ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(options_, st.st_size, in));

    ceph::bufferlist result;
    s = doa_->Exec(st.st_ino, "scan_op", in, result);
    if (!s.ok()) {
      return Status::ExecutionError(s.message());
    }

    RecordBatchVector batches;
    auto buffer = std::make_shared<Buffer>((uint8_t*)result.c_str(), result.length());
    auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
    auto options = ipc::IpcReadOptions::Defaults();
    options.use_threads = false;
    ARROW_ASSIGN_OR_RAISE(auto rb_reader, arrow::ipc::RecordBatchStreamReader::Open(
                                              buffer_reader, options));
    RecordBatchVector rbatches;
    rb_reader->ReadAll(&rbatches);
    return MakeVectorIterator(rbatches);
  }

 protected:
  FileSource source_;
  std::shared_ptr<DirectObjectAccess> doa_;
};

RadosParquetFileFormat::RadosParquetFileFormat(const std::string& ceph_config_path,
                                               const std::string& data_pool,
                                               const std::string& user_name,
                                               const std::string& cluster_name) {
  arrow::dataset::RadosCluster::RadosConnectionCtx ctx;
  ctx.ceph_config_path = "/etc/ceph/ceph.conf";
  ctx.data_pool = "cephfs_data";
  ctx.user_name = "client.admin";
  ctx.cluster_name = "ceph";
  ctx.cls_name = "arrow";
  auto cluster = std::make_shared<RadosCluster>(ctx);
  cluster->Connect();
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>(cluster);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> RadosParquetFileFormat::Inspect(
    const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source));
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

Result<ScanTaskIterator> RadosParquetFileFormat::ScanFile(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  std::shared_ptr<ScanOptions> options_ = std::make_shared<ScanOptions>(*options);
  options_->partition_expression = file->partition_expression();
  options_->dataset_schema = file->dataset_schema();
  ScanTaskVector v{std::make_shared<RadosParquetScanTask>(
      std::move(options_), std::move(file), file->source(), std::move(doa_))};
  return MakeVectorIterator(v);
}


Status SerializeScanRequestToBufferlist(std::shared_ptr<ScanOptions> options,
                                        int64_t file_size, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto filter, compute::Serialize(options->filter));
  ARROW_ASSIGN_OR_RAISE(auto partition,
                        compute::Serialize(options->partition_expression));
  ARROW_ASSIGN_OR_RAISE(auto projection,
                        ipc::SerializeSchema(*options->projected_schema));
  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::SerializeSchema(*options->dataset_schema));

  flatbuffers::FlatBufferBuilder builder(1024);

  auto filter_vec = builder.CreateVector(filter->data(), filter->size());
  auto partition_vec = builder.CreateVector(partition->data(), partition->size());
  auto projected_schema_vec =
      builder.CreateVector(projection->data(), projection->size());
  auto dataset_schema_vec = builder.CreateVector(schema->data(), schema->size());

  auto request = flatbuf::CreateRequest(builder, file_size, filter_vec, partition_vec,
                                        dataset_schema_vec, projected_schema_vec);
  builder.Finish(request);
  uint8_t* buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  bl.append((char*)buf, size);
  return Status::OK();
}

Status DeserializeScanRequestFromBufferlist(compute::Expression* filter,
                                            compute::Expression* partition,
                                            std::shared_ptr<Schema>* projected_schema,
                                            std::shared_ptr<Schema>* dataset_schema,
                                            int64_t& file_size, ceph::bufferlist& bl) {
  auto request = flatbuf::GetRequest((uint8_t*)bl.c_str());

  ARROW_ASSIGN_OR_RAISE(auto filter_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->filter()->data(), request->filter()->size())));
  *filter = filter_;

  ARROW_ASSIGN_OR_RAISE(auto partition_,
                        compute::Deserialize(std::make_shared<Buffer>(
                            request->partition()->data(), request->partition()->size())));
  *partition = partition_;

  ipc::DictionaryMemo empty_memo;
  io::BufferReader projection_reader(request->projection_schema()->data(),
                                     request->projection_schema()->size());
  io::BufferReader schema_reader(request->dataset_schema()->data(),
                                 request->dataset_schema()->size());

  ARROW_ASSIGN_OR_RAISE(auto projected_schema_,
                        ipc::ReadSchema(&projection_reader, &empty_memo));
  *projected_schema = projected_schema_;

  ARROW_ASSIGN_OR_RAISE(auto dataset_schema_,
                        ipc::ReadSchema(&schema_reader, &empty_memo));
  *dataset_schema = dataset_schema_;

  file_size = request->file_size();
  return Status::OK();
}

Status SerializeTableToBufferlist(std::shared_ptr<Table>& table, ceph::bufferlist& bl) {
  ARROW_ASSIGN_OR_RAISE(auto buffer_output_stream, io::BufferOutputStream::Create());

  ipc::IpcWriteOptions options = ipc::IpcWriteOptions::Defaults();
  ARROW_ASSIGN_OR_RAISE(
      options.codec,
      util::Codec::Create(Compression::LZ4_FRAME, std::numeric_limits<int>::min()));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, ipc::MakeStreamWriter(buffer_output_stream, table->schema(), options));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(writer->Close());

  ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_output_stream->Finish());
  bl.append((char*)buffer->data(), buffer->size());
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
