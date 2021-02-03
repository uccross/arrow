#!/usr/bin/env bash
#
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

set -ex

source_dir=${1}/cpp
build_dir=${2}/cpp
test_dir=${build_dir}/test-cluster

# pushd ${build_dir}

# get rid of process and directories leftovers
pkill ceph-mon || true
pkill ceph-osd || true
rm -fr ${test_dir}

# cluster wide parameters
mkdir -p ${test_dir}/log
cat >> /etc/ceph/ceph.conf <<EOF
[global]
fsid = $(uuidgen)
osd crush chooseleaf type = 0
run dir = ${test_dir}/run
auth cluster required = none
auth service required = none
auth client required = none
osd pool default size = 1
EOF
export CEPH_ARGS="--conf /etc/ceph/ceph.conf"

# single monitor
MON_DATA=${test_dir}/mon
mkdir -p $MON_DATA

cat >> /etc/ceph/ceph.conf <<EOF
[mon.0]
log file = ${test_dir}/log/mon.log
chdir = ""
mon cluster log file = ${test_dir}/log/mon-cluster.log
mon data = ${MON_DATA}
mon addr = 127.0.0.1
# this was added to enable pool deletion within method delete_one_pool_pp()
mon_allow_pool_delete = true
EOF

ceph-mon --id 0 --mkfs --keyring /dev/null
touch ${MON_DATA}/keyring
cp ${MON_DATA}/keyring /etc/ceph/keyring
ceph-mon --id 0

# single osd
OSD_DATA=${test_dir}/osd
mkdir ${OSD_DATA}

cat >> /etc/ceph/ceph.conf <<EOF
[osd.0]
log file = ${test_dir}/log/osd.log
chdir = ""
osd data = ${OSD_DATA}
osd journal = ${OSD_DATA}.journal
osd journal size = 100
osd objectstore = memstore
osd class load list = lock log numops refcount replica_log statelog timeindex user version arrow
EOF

OSD_ID=$(ceph osd create)
ceph osd crush add osd.${OSD_ID} 1 root=default host=localhost
ceph-osd --id ${OSD_ID} --mkjournal --mkfs
ceph-osd --id ${OSD_ID}

# start a MDS daemon
MDS_DATA=${TEST_DIR}/mds
mkdir -p $MDS_DATA

ceph osd pool create cephfs_data 16
ceph osd pool create cephfs_metadata 16
ceph fs new cephfs cephfs_metadata cephfs_data

ceph-mds --id a
while [[ ! $(ceph mds stat | grep "up:active") ]]; do sleep 1; done

# start a MGR daemon
ceph-mgr --id 0

export CEPH_CONF="/etc/ceph/ceph.conf"

# mkdir -p /usr/lib/x86_64-linux-gnu/rados-classes/
# mkdir -p /usr/lib/aarch64-linux-gnu/rados-classes/
# cp debug/libcls_arrow* /usr/lib/x86_64-linux-gnu/rados-classes/
# cp debug/libcls_arrow* /usr/lib/aarch64-linux-gnu/rados-classes/

apt-get update
apt-get install -y ceph-fuse
mkdir -p /mnt/cephfs
ceph-fuse --id client.admin -m 127.0.0.1:6789 --client_fs cephfs /mnt/cephfs

# TESTS=debug/arrow-cls-cls-arrow-test
# if [ -f "$TESTS" ]; then
#     debug/arrow-cls-cls-arrow-test
# fi

# popd
