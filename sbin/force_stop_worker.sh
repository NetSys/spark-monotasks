#!/usr/bin/env bash

#
# Copyright 2014 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Kills Spark Worker processes.

pid=`jps -l | grep "org.apache.spark.deploy.worker.Worker" | cut -d " " -f 1`
if [[ -z $pid ]]; then
  echo "No Worker process running"
else
  echo "Killing Worker with pid $pid"
  kill -9 $pid
fi
