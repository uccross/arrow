#!/bin/bash
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

set -exu

if [ $# -lt 2 ]; then
  echo "Usage: $0 VERSION rc"
  echo "       $0 VERSION rc BINTRAY_REPOSITORY"
  echo "       $0 VERSION release"
  echo "       $0 VERSION local"
  echo " e.g.: $0 0.13.0 rc           # Verify 0.13.0 RC"
  echo " e.g.: $0 0.13.0 release      # Verify 0.13.0"
  echo " e.g.: $0 0.13.0 rc kszucs/arrow # Verify 0.13.0 RC at https://bintray.com/kszucs/arrow"
  echo " e.g.: $0 0.13.0-dev20210203 local # Verify 0.13.0-dev20210203 on local"
  exit 1
fi

VERSION="$1"
TYPE="$2"

local_prefix="/arrow/dev/tasks/linux-packages"

artifactory_base_url="https://apache.jfrog.io/artifactory/arrow/centos"
if [ "${TYPE}" = "rc" ]; then
  artifactory_base_url+="-rc"
fi

distribution=$(. /etc/os-release && echo "${ID}")
distribution_version=$(. /etc/os-release && echo "${VERSION_ID}")

cmake_package=cmake
cmake_command=cmake
have_flight=yes
have_gandiva=yes
have_glib=yes
have_parquet=yes
install_command="dnf install -y --enablerepo=powertools"
case "${distribution}-${distribution_version}" in
  centos-7)
    cmake_package=cmake3
    cmake_command=cmake3
    have_flight=no
    have_gandiva=no
    install_command="yum install -y"
    ;;
esac
if [ "$(arch)" = "aarch64" ]; then
  have_gandiva=no
fi

if [ "${TYPE}" = "local" ]; then
  case "${VERSION}" in
    *-dev*)
      package_version="$(echo "${VERSION}" | sed -e 's/-dev\(.*\)$/-0.dev\1/g')"
      ;;
    *-rc*)
      package_version="$(echo "${VERSION}" | sed -e 's/-rc.*$//g')"
      package_version+="-1"
      ;;
    *)
      package_version="${VERSION}-1"
      ;;
  esac
  package_version+=".el${distribution_version}"
  release_path="${local_prefix}/yum/repositories"
  release_path+="/centos/${distribution_version}/$(arch)/Packages"
  release_path+="/apache-arrow-release-${package_version}.noarch.rpm"
  ${install_command} "${release_path}"
else
  package_version="${VERSION}"
  if [ $# -eq 3 ]; then
    ${install_command} \
      https://dl.bintray.com/$3/centos-rc/${distribution_version}/apache-arrow-release-latest.rpm
  else
    ${install_command} \
      ${artifactory_base_url}/${distribution_version}/apache-arrow-release-latest.rpm
  fi
fi

if [ "${TYPE}" = "local" ]; then
  sed \
    -i"" \
    -e "s,baseurl=https://apache\.jfrog\.io/artifactory/arrow/,baseurl=file://${local_prefix}/yum/repositories/,g" \
    /etc/yum.repos.d/Apache-Arrow.repo
  keys="${local_prefix}/KEYS"
  if [ -f "${keys}" ]; then
    cp "${keys}" /etc/pki/rpm-gpg/RPM-GPG-KEY-Apache-Arrow
  fi
else
  if [ "${TYPE}" = "rc" ]; then
    if [ $# -eq 3 ]; then
      sed \
        -i"" \
        -e "s,baseurl=https://apache\.jfrog\.io/artifactory/arrow/centos/,baseurl=https://dl.bintray.com/$3/centos-rc/,g" \
        /etc/yum.repos.d/Apache-Arrow.repo
    else
      sed \
        -i"" \
        -e "s,/centos/,/centos-rc/,g" \
        /etc/yum.repos.d/Apache-Arrow.repo
    fi
  fi
fi

${install_command} --enablerepo=epel arrow-devel-${package_version}
${install_command} \
  ${cmake_package} \
  gcc-c++ \
  git \
  make
mkdir -p build
cp -a /arrow/cpp/examples/minimal_build build
pushd build/minimal_build
${cmake_command} .
make -j$(nproc)
./arrow_example
popd

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-glib-devel-${package_version}
  ${install_command} --enablerepo=epel arrow-glib-doc-${package_version}
fi
${install_command} --enablerepo=epel arrow-python-devel-${package_version}

if [ "${have_glib}" = "yes" ]; then
  ${install_command} --enablerepo=epel plasma-glib-devel-${package_version}
  ${install_command} --enablerepo=epel plasma-glib-doc-${package_version}
else
  ${install_command} --enablerepo=epel plasma-devel-${package_version}
fi

if [ "${have_flight}" = "yes" ]; then
  ${install_command} --enablerepo=epel arrow-flight-devel-${package_version}
fi

if [ "${have_gandiva}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel gandiva-glib-devel-${package_version}
    ${install_command} --enablerepo=epel gandiva-glib-doc-${package_version}
  else
    ${install_command} --enablerepo=epel gandiva-devel-${package_version}
  fi
fi

if [ "${have_parquet}" = "yes" ]; then
  if [ "${have_glib}" = "yes" ]; then
    ${install_command} --enablerepo=epel parquet-glib-devel-${package_version}
    ${install_command} --enablerepo=epel parquet-glib-doc-${package_version}
  else
    ${install_command} --enablerepo=epel parquet-devel-${package_version}
  fi
fi
