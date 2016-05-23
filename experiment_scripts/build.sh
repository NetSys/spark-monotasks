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

# This script stops Spark, rebuilds Spark, copies the new version to
# the cluster, and then re-starts Spark.

set -e
pushd /root/spark
echo "Stopping Spark"
sbin/stop-all.sh

if [ "$#" -eq 1 ] && [ $1 == "install" ]; then
    echo "Building and installing Spark"
    build/mvn package install -DskipTests -Phive -Phive-thriftserver -Dhadoop.version=2.0.0-cdh4.2.0
else
    echo "Building Spark"
    build/mvn package -DskipTests -Phive -Phive-thriftserver -Dhadoop.version=2.0.0-cdh4.2.0
fi

echo "Copying Spark"
/root/spark-ec2/copy-dir --delete /root/spark

echo "Starting Spark"
sbin/start-all.sh

popd
