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
#define _FILE_OFFSET_BITS 64
#pragma once

#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/rados.h"
#include "arrow/dataset/rados_utils.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include <cephfs/ceph_ll_client.h>
#include <cephfs/libcephfs.h>

namespace arrow {
namespace dataset {

class ARROW_DS_EXPORT RadosDatasetFactoryOptions : public FileSystemFactoryOptions {
 public:
  std::string pool_name;
  std::string user_name;
  std::string cluster_name;
  std::string ceph_config_path;
  uint64_t flags;
  std::string cls_name;
  int64_t format = 2;
};

class ARROW_DS_EXPORT RadosCluster {
 public:
  RadosCluster(std::string pool, std::string conf_path)
      : pool_name(pool),
        user_name("client.admin"),
        cluster_name("ceph"),
        ceph_config_path(conf_path),
        flags(0),
        cls_name("arrow"),
        rados(new RadosWrapper()),
        ioCtx(new IoCtxWrapper()) {}

  Status Connect() {
    if (rados->init2(user_name.c_str(), cluster_name.c_str(), flags))
      return Status::Invalid("librados::init2 returned non-zero exit code.");

    if (rados->conf_read_file(ceph_config_path.c_str()))
      return Status::Invalid("librados::conf_read_file returned non-zero exit code.");

    if (rados->connect())
      return Status::Invalid("librados::connect returned non-zero exit code.");

    if (rados->ioctx_create(pool_name.c_str(), ioCtx))
      return Status::Invalid("librados::ioctx_create returned non-zero exit code.");

    return Status::OK();
  }

  Status Disconnect() {
    rados->shutdown();
    return Status::OK();
  }

  std::string pool_name;
  std::string user_name;
  std::string cluster_name;
  std::string ceph_config_path;
  uint64_t flags;
  std::string cls_name;

  RadosInterface* rados;
  IoCtxInterface* ioCtx;
};

class ARROW_DS_EXPORT RadosFileSystem : public fs::LocalFileSystem {
 public:
  Status Init(std::shared_ptr<RadosCluster> cluster) {
    cluster_ = cluster;
    const char id[] = "client.admin";

    if (ceph_create(&cmount_, cluster->user_name.c_str()))
      return Status::Invalid("libcephfs::ceph_create returned non-zero exit code.");

    if (ceph_conf_read_file(cmount_, cluster->ceph_config_path.c_str()))
      return Status::Invalid(
          "libcephfs::ceph_conf_read_file returned non-zero exit code.");

    if (ceph_init(cmount_))
      return Status::Invalid("libcephfs::ceph_init returned non-zero exit code.");

    if (ceph_select_filesystem(cmount_, "cephfs"))
      return Status::Invalid(
          "libcephfs::ceph_select_filesystem returned non-zero exit code.");

    if (ceph_mount(cmount_, "/"))
      return Status::Invalid("libcephfs::ceph_mount returned non-zero exit code.");

    return Status::OK();
  }

  std::string type_name() { return "rados"; }

  int64_t Write(const std::string& path, std::shared_ptr<Buffer> buffer) {
    std::string dirname = arrow::fs::internal::GetAbstractPathParent(path).first;
    if (!CreateDir(dirname).ok()) return -1;

    int fd = ceph_open(cmount_, path.c_str(), O_WRONLY | O_CREAT, 0777);
    if (fd < 0) return fd;

    int num_bytes_written =
        ceph_write(cmount_, fd, (char*)buffer->data(), buffer->size(), 0);

    if (int e = ceph_close(cmount_, fd)) return e;

    return num_bytes_written;
  }

  Status CreateDir(const std::string& path, bool recursive = true) {
    if (recursive) {
      if (ceph_mkdirs(cmount_, path.c_str(), 0666))
        return Status::IOError("libcephfs::ceph_mkdirs returned non-zero exit code.");
    } else {
      if (ceph_mkdir(cmount_, path.c_str(), 0666))
        return Status::IOError("libcephfs::ceph_mkdir returned non-zero exit code.");
    }
    return Status::OK();
  }

  Status DeleteDir(const std::string& path) {
    if (ceph_rmdir(cmount_, path.c_str()))
      return Status::IOError("libcephfs::ceph_rmdir returned non-zero exit code.");
    return Status::OK();
  }

  Status DeleteFile(const std::string& path) {
    if (ceph_unlink(cmount_, path.c_str()))
      return Status::IOError("libcephfs::ceph_unlink returned non-zero exit code.");
    return Status::OK();
  }

  Status DeleteFiles(const std::vector<std::string>& paths) {
    Status s;
    for (auto& path : paths) {
      if (!DeleteFile(path).ok())
        return Status::IOError(
            "RadosFileSystem::DeleteFiles returned non-zero exit code.");
    }
    return Status::OK();
  }

  Status Exec(const std::string& path, const std::string& fn, librados::bufferlist& in,
              librados::bufferlist& out) {
    struct ceph_statx stx;
    if (ceph_statx(cmount_, path.c_str(), &stx, 0, 0))
      return Status::IOError("ceph_stat failed");

    uint64_t inode = stx.stx_ino;

    std::stringstream ss;
    ss << std::hex << inode;
    std::string oid(ss.str() + ".00000000");

    if (cluster_->ioCtx->exec(oid.c_str(), cluster_->cls_name.c_str(), fn.c_str(), in,
                              out)) {
      return Status::ExecutionError("librados::exec returned non-zero exit code.");
    }

    return Status::OK();
  }

  Status ListDirRecursive(const std::string& path,
                          std::vector<std::string>& files) {
    struct dirent* de = NULL;
    struct ceph_dir_result *dirr = NULL;
    
    if (ceph_opendir(cmount_, path.c_str(), &dirr))
      return Status::IOError("libcephfs::ceph_opendir returned non-zero exit code.");

    while ((de = ceph_readdir(cmount_, dirr)) != NULL) {
      std::string entry(de->d_name);

      if (de->d_type == DT_REG) {
        ARROW_LOG(INFO) << path + "/" + entry << "\n";
        files.push_back(path + "/" + entry);
      } else {
        if (entry == "." || entry == "..") continue;
        ListDirRecursive(path + "/" + entry, files);
      }
    }

    if (ceph_closedir(cmount_, dirr))
      return Status::IOError("libcephfs::ceph_closedir returned non-zero exit code.");

    return Status::OK();
  }

  Status ListDir(const std::string& path, std::vector<std::string>& files) {
    return ListDirRecursive(path, files);
  }

 protected:
  std::shared_ptr<RadosCluster> cluster_;
  struct ceph_mount_info* cmount_;
};

class ARROW_DS_EXPORT RadosFragment : public Fragment {
 public:
  RadosFragment(std::shared_ptr<Schema> schema, std::string path,
                std::shared_ptr<RadosFileSystem> filesystem, int64_t format,
                std::shared_ptr<Expression> partition_expression = scalar(true))
      : Fragment(partition_expression, std::move(schema)),
        path_(std::move(path)),
        filesystem_(std::move(filesystem)),
        format_(format) {}

  Result<ScanTaskIterator> Scan(std::shared_ptr<ScanOptions> options,
                                std::shared_ptr<ScanContext> context) override;

  std::string type_name() const override { return "rados"; }

  bool splittable() const override { return false; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override;
  std::string path_;
  std::shared_ptr<RadosFileSystem> filesystem_;
  int64_t format_;
};

using RadosFragmentVector = std::vector<std::shared_ptr<RadosFragment>>;

class ARROW_DS_EXPORT RadosDataset : public Dataset {
 public:
  static Result<std::shared_ptr<Dataset>> Make(
      std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
      std::shared_ptr<RadosFileSystem> filesystem);

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  std::string type_name() const override { return "rados"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(
      std::shared_ptr<Schema> schema) const override;

 protected:
  RadosDataset(std::shared_ptr<Schema> schema, RadosFragmentVector fragments,
               std::shared_ptr<RadosFileSystem> filesystem)
      : Dataset(std::move(schema)),
        fragments_(fragments),
        filesystem_(std::move(filesystem)) {}

  FragmentIterator GetFragmentsImpl(
      std::shared_ptr<Expression> predicate = scalar(true)) override;
  RadosFragmentVector fragments_;
  std::shared_ptr<RadosFileSystem> filesystem_;
};

class ARROW_DS_EXPORT RadosScanTask : public ScanTask {
 public:
  RadosScanTask(std::shared_ptr<ScanOptions> options,
                std::shared_ptr<ScanContext> context, std::string path,
                std::shared_ptr<RadosFileSystem> filesystem)
      : ScanTask(std::move(options), std::move(context)),
        path_(std::move(path)),
        filesystem_(std::move(filesystem)) {}

  Result<RecordBatchIterator> Execute();

 protected:
  std::string path_;
  std::shared_ptr<RadosFileSystem> filesystem_;
};

class ARROW_DS_EXPORT RadosDatasetFactory : public DatasetFactory {
 public:
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::shared_ptr<RadosFileSystem> filesystem, RadosDatasetFactoryOptions options);

  Result<std::vector<std::shared_ptr<Schema>>> InspectSchemas(InspectOptions options);

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;

 protected:
  RadosDatasetFactory(std::vector<std::string> paths,
                      std::shared_ptr<RadosFileSystem> filesystem,
                      RadosDatasetFactoryOptions options)
      : paths_(paths), filesystem_(std::move(filesystem)), options_(std::move(options)) {}
  std::vector<std::string> paths_;
  std::shared_ptr<RadosFileSystem> filesystem_;
  RadosDatasetFactoryOptions options_;
};

class SplittedParquetWriter {
 public:
  SplittedParquetWriter(std::shared_ptr<RadosFileSystem> filesystem)
      : filesystem_(std::move(filesystem)) {}

  Status WriteTable(std::shared_ptr<Table> table, std::string path) {
    librados::bufferlist bl;

    ARROW_ASSIGN_OR_RAISE(auto bos, io::BufferOutputStream::Create());

    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), bos, 1));

    ARROW_ASSIGN_OR_RAISE(auto buffer, bos->Finish());

    int64_t num_bytes_written = filesystem_->Write(path, buffer);
    if (num_bytes_written < 0)
      return Status::IOError(
          "SplittedParquetWriter::WriteTable returned non-zero exit code.");
    return Status::OK();
  }

 protected:
  std::shared_ptr<RadosFileSystem> filesystem_;
};

}  // namespace dataset
}  // namespace arrow
