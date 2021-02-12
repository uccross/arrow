# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.rados import SplittedParquetWriter


def test_dataset_discovery():
    rados_parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        format=ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf")
    )
    parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        format="parquet"
    )
    assert len(rados_parquet_dataset.files) == len(parquet_dataset.files)
    assert len(rados_parquet_dataset.files) == 8
    assert rados_parquet_dataset.schema == parquet_dataset.schema


def test_without_partition_pruning():
    rados_parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        format=ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf")
    )
    parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        format="parquet"
    )

    rados_parquet_df = rados_parquet_dataset.to_table(
        columns=['DOLocationID', 'total_amount', 'fare_amount'],
        filter=(ds.field('total_amount') > 200)).to_pandas()
    parquet_df = parquet_dataset.to_table(
        columns=['DOLocationID', 'total_amount', 'fare_amount'],
        filter=(ds.field('total_amount') > 200)).to_pandas()

    assert rados_parquet_df.equals(parquet_df) == 1


def test_with_partition_pruning():
    filter_expression = (
        (ds.field('tip_amount') > 10) &
        (ds.field('payment_type') > 2) &
        (ds.field('VendorID') > 1)
    )
    projection_cols = ['payment_type', 'tips_amount', 'VendorID']
    partitioning = ds.partitioning(
        pa.schema([("payment_type", pa.int32()), ("VendorID", pa.int32())]),
        flavor="hive"
    )

    rados_parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        partitioning=partitioning,
        format=ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf")
    )
    parquet_dataset = ds.dataset(
        "file:///mnt/cephfs/nyc/", 
        partitioning=partitioning,
        format="parquet"
    )

    rados_parquet_df = rados_parquet_dataset.to_table(
        columns=projection_cols, filter=filter_expression).to_pandas()
    
    parquet_df = rados_parquet_dataset.to_table(
        columns=projection_cols, filter=filter_expression).to_pandas()
    
    assert rados_parquet_df.equals(parquet_df) == 1


def test_splitted_parquet_writer():
    chunksize = 4 * 1000000  # 4MB
    writer = SplittedParquetWriter("largefile.parquet", os.getcwd(), chunksize
                                   )
    writer.write()
    num_files_written = writer.close()
    assert num_files_written == 5

    original_file_rows = pq.read_table('largefile.parquet').num_rows
    splitted_files_rows = 0
    for i in range(num_files_written):
        splitted_files_rows += pq.read_metadata(f"file.{i}.parquet").num_rows

    assert splitted_files_rows == original_file_rows


if __name__ == "__main__":
    test_dataset_discovery()
    test_without_partition_pruning()
    test_with_partition_pruning()
    test_splitted_parquet_writer()
    # unmount cephfs for graceful exit of the container
    os.system("umount -f /mnt/cephfs")
