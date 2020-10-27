#include <iostream>
#include <errno.h>

#include <rados/librados.hpp>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "test_utils.h"
#include "gtest/gtest.h"

using arrow::dataset::string_literals::operator"" _;

int create_test_arrow_table(std::shared_ptr<arrow::Table> *out_table) {

  // create a memory pool
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // arrow array builder for each table column
  arrow::Int32Builder id_builder(pool);
  arrow::DoubleBuilder cost_builder(pool);
  arrow::ListBuilder components_builder(
      pool, std::make_shared<arrow::DoubleBuilder>(pool));

  // The following builder is owned by components_builder.
  arrow::DoubleBuilder &cost_components_builder = *(
      static_cast<arrow::DoubleBuilder *>(components_builder.value_builder()));

  // append some fake data
  for (int i = 0; i < 10; ++i) {
    id_builder.Append(i);
    cost_builder.Append(i + 1.0);
    // Indicate the start of a new list row. This will memorise the current
    // offset in the values builder.
    components_builder.Append();
    std::vector<double> nums;
    nums.push_back(i + 1.0);
    nums.push_back(i + 2.0);
    nums.push_back(i + 3.0);
    cost_components_builder.AppendValues(nums.data(), nums.size());
  }

  // At the end, we finalise the arrays, declare the (type) schema and combine
  // them into a single `arrow::Table`:
  std::shared_ptr<arrow::Int32Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::DoubleArray> cost_array;
  cost_builder.Finish(&cost_array);

  // No need to invoke cost_components_builder.Finish because it is implied by
  // the parent builder's Finish invocation.
  std::shared_ptr<arrow::ListArray> cost_components_array;
  components_builder.Finish(&cost_components_array);

  // create table schema and make table from col arrays and schema
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  *out_table =
      arrow::Table::Make(schema, {id_array, cost_array, cost_components_array});

  if (*out_table == nullptr) {
    return -1;
  }
  return 0;
}

TEST(ClsSDK, TestWriteAndReadTable) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  arrow::dataset::serialize_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_1", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});
  arrow::dataset::serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_1", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  arrow::dataset::deserialize_table_from_bufferlist(&table_, out_);
  ASSERT_EQ(table->Equals(*table_), 1);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestProjection) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  arrow::dataset::serialize_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_2", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  auto filter = arrow::dataset::scalar(true);
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});

  auto table_projected = table->RemoveColumn(1).ValueOrDie();
  arrow::dataset::serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_2", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  arrow::dataset::deserialize_table_from_bufferlist(&table_, out_);

  ASSERT_EQ(table->Equals(*table_), 0);
  ASSERT_EQ(table_projected->Equals(*table_), 1);
  ASSERT_EQ(table_->num_columns(), 2);
  ASSERT_EQ(table_->schema()->Equals(*schema), 1);

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestSelection) {
  librados::Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // WTITE PATH
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  arrow::dataset::serialize_table_to_bufferlist(table, in);
  ASSERT_EQ(0, ioctx.exec("test_object_3", "arrow", "write", in, out));

  // READ PATH
  librados::bufferlist in_, out_;
  std::shared_ptr<arrow::dataset::Expression> filter = ("id"_ == int32_t(8) || "id"_ == int32_t(7)).Copy();
  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))});
  arrow::dataset::serialize_scan_request_to_bufferlist(filter, schema, in_);
  ASSERT_EQ(0, ioctx.exec("test_object_3", "arrow", "read", in_, out_));
  std::shared_ptr<arrow::Table> table_;
  arrow::dataset::deserialize_table_from_bufferlist(&table_, out_);
  ASSERT_EQ(table_->num_rows(), 2);
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(ClsSDK, TestEndToEnd) {
  librados::Rados cluster;
  std::string pool_name = "test-pool";
  create_one_pool_pp(pool_name, cluster);
  librados::IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  // create and write table
  librados::bufferlist in, out;
  std::shared_ptr<arrow::Table> table;
  create_test_arrow_table(&table);
  arrow::dataset::serialize_table_to_bufferlist(table, in);

  arrow::TableBatchReader table_reader(*table);
  arrow::RecordBatchVector batches;
  table_reader.ReadAll(&batches);

  for (int i = 0; i < 4; i++) {
    std::string obj_id = "obj." + std::to_string(i);
    ASSERT_EQ(0, ioctx.exec(obj_id, "arrow", "write", in, out));
  }

  auto schema = arrow::schema({
      arrow::field("id", arrow::int32()),
      arrow::field("cost", arrow::float64()),
      arrow::field("cost_components", arrow::list(arrow::float64()))
    });

    arrow::dataset::RadosObjectVector objects;
    for (int i = 0; i < 1; i++) objects.push_back(std::make_shared<arrow::dataset::RadosObject>("obj." + std::to_string(i)));

    auto rados_options = arrow::dataset::RadosOptions::FromPoolName("test-pool");

    auto rados_ds = std::make_shared<arrow::dataset::RadosDataset>(schema, objects, rados_options);
    auto inmemory_ds = std::make_shared<arrow::dataset::InMemoryDataset>(schema, batches);

    auto context = std::make_shared<arrow::dataset::ScanContext>();

    auto rados_scanner_builder = rados_ds->NewScan().ValueOrDie();
    auto inmemory_scanner_builder = inmemory_ds->NewScan().ValueOrDie();

    rados_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
    rados_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
    auto rados_scanner = rados_scanner_builder->Finish().ValueOrDie();

    inmemory_scanner_builder->Filter(("id"_ > int32_t(7)).Copy());
    inmemory_scanner_builder->Project(std::vector<std::string>{"cost", "id"});
    auto inmemory_scanner = inmemory_scanner_builder->Finish().ValueOrDie();

    auto expected_table = inmemory_scanner->ToTable().ValueOrDie();
    auto result_table = rados_scanner->ToTable().ValueOrDie();

    ASSERT_EQ(result_table->Equals(*expected_table), 1);
}