#!/bin/bash
#
# Copyright 2016 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This file loads the necessary dependencies and compiles the ML matrix
# libraries on an ec2 cluster.
# This script assumes that Spark has already been built and installed
# (using mvn install) with branch 1.3.1-SNAPSHOT.

# Terminate if any commands fail.
set -e

pushd /root/

# Instructions for this portion of the setup can be found here:
# https://github.com/amplab/ml-matrix/blob/master/EC2.md

if [ ! -d "/root/matrix-bench" ]; then
  echo "Cloning matrix-bench to do BLAS setup"
  git clone https://github.com/shivaram/matrix-bench.git
fi

echo "Building OpenBLAS"
# One of the commands called by this script fails if it's not called from the
# root directory, so navigate there first.
pushd /root
bash matrix-bench/setup-ec2.sh
popd

echo "Copying OpenBLAS install to slaves"
/root/spark-ec2/copy-dir /root/openblas-install

echo "Configuring BLAS on slaves"
/root/spark/sbin/slaves.sh rm /etc/ld.so.conf.d/atlas-x86_64.conf
/root/spark/sbin/slaves.sh ldconfig

# This step must be done AFTER each slave has received the openblas-install folder
# that was copied above.
/root/spark/sbin/slaves.sh ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/liblapack.so.3
/root/spark/sbin/slaves.sh ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/libblas.so.3

echo "Configuring BLAS on master"
rm /etc/ld.so.conf.d/atlas-x86_64.conf
ldconfig
ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/liblapack.so.3
ln -sf /root/openblas-install/lib/libblas.so.3 /usr/lib64/libblas.so.3

echo "Checking that the correct BLAS was linked. Output should have sandybridge in it."
/root/spark/sbin/slaves.sh readlink -e /usr/lib64/libblas.so.3

# Remind the user about some extra manual configuration.
echo "Be sure to change spark-defaults.conf to include:"
echo "spark.executor.extraClassPath /root/ml-matrix/target/scala-2.10/mlmatrix-assembly-0.1.jar:/root/ephemeral-hdfs/conf"
echo "Also don't forget to set export OMP_NUM_THREADS=1 in spark-env.sh"

popd
