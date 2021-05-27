<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Installing SkyhookDM

1. If you don't already have a Ceph cluster, please follow [this](https://blog.risingstack.com/ceph-storage-deployment-vm/) guide to create one. You may also run [this](../scripts/deploy_ceph.sh) script for a 3 OSD Ceph cluster and a CephFS mount.

2. Build and install SkyhookDM and [PyArrow](https://pypi.org/project/pyarrow/) (with Rados Parquet extensions) using [this](../scripts/deploy_skyhook.sh) script.

3. Update your Ceph configuration file with the below line and restart the OSD daemons to load the updated configuration.
```
osd class load list = *
```

# Interacting with SkyhookDM

1. Write some [Parquet](https://parquet.apache.org/) files in the CephFS mount by splitting them up into files of size `128 MB` or less. This is achieved by using the [`SplittedParquetWriter`](../../../../../../python/pyarrow/rados.py) API. An example script to split up Parquet files into SkyhookDM compatible files can be found [here](../scripts/splitter.py).

2. Write a client script and get started with querying datasets in SkyhookDM. An example script is given below.
```python
import pyarrow.dataset as ds

format_ = ds.RadosParquetFileFormat("/path/to/cephconfig", "cephfs-data-pool-name")
dataset_ = ds.dataset("file:///mnt/cephfs/dataset", format=format_)
print(dataset_.to_table())
```
