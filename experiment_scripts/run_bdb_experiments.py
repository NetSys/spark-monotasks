"""
This script runs the AMPLab Big Data Benchmark on an EC2 cluster. It assumes that the benchmark data
has already been loaded onto the cluster.

Run this script with the "-h" flag for usage information.
"""

import argparse
import os
import subprocess
import sys
import time

import utils

def main(argv):
  aws_key_id, aws_key = get_env_vars(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])
  args = parse_args(argv)
  print_info(args)

  is_first_branch = True
  for branch in args.branches:
    execute_queries_for_branch(aws_key_id, aws_key, args, branch, is_first_branch)
    is_first_branch = False
  print_heading("Done!")

def parse_args(args):
  parser = argparse.ArgumentParser(description="Runs Big Data Benchmark experiments on EC2.")
  parser.add_argument(
    "-b",
    "--benchmark-directory",
    dest="benchmark_dir",
    help="The local path to the \"benchmark\" repository.",
    metavar="BENCHMARK-DIRECTORY",
    required=True)
  parser.add_argument(
    "-m",
    "--scripts-directory",
    dest="scripts_dir",
    help="The local path to the \"monotasks-scripts\" repository.",
    metavar="SCRIPTS-DIRECTORY",
    required=True)
  parser.add_argument(
    "-d",
    "--driver-address",
    dest="driver_addr",
    help="The address of the Spark driver. On the driver, Spark must be located at /root/spark.",
    metavar="DRIVER-ADDRESS",
    required=True)
  parser.add_argument(
    "-e",
    "--executor-address",
    dest="executor_addr",
    help="The address of a Spark executor.",
    metavar="EXECUTOR-ADDRESS",
    required=True)
  parser.add_argument(
    "-i",
    "--identity-file",
    dest="identity_file",
    help="The cluster's identity file.",
    metavar="IDENTITY-FILE",
    required=True)
  parser.add_argument(
    "-j",
    "--jar-directory",
    dest="jar_dir",
    help="The location on the Spark driver where pre-compiled JARs are stored. Not specifying this \
      option leads to recompilation. The JAR for a branch named \"branch\" should be stored in the \
      directory %(metavar)s/branch/",
    metavar="JAR-DIRECTORY")
  parser.add_argument(
    "-o",
    "--output-directory",
    dest="output_dir",
    help="The local directory in which the resulting log files will be stored. Log files for a \
      particular combination of query and branch are stored in the directory \
      %(metavar)s/query/branch/",
    metavar="OUTPUT-DIRECTORY",
    required=True)
  parser.add_argument(
    "-s",
    "--scale-factor",
    dest="scale_factor",
    help="Which Big Data Benchmark scale factor to use.",
    metavar="SCALE-FACTOR",
    required=True,
    type=int)
  parser.add_argument(
    "-f",
    "--file-format",
    choices=["sequence", "sequence-snappy", "text", "text-deflate"],
    dest="file_format",
    help="The file format of the input data.",
    metavar="FILE-FORMAT",
    required=True)
  parser.add_argument(
    "-p",
    "--parquet",
    action="store_true",
    default=False,
    dest="parquet",
    help="Indicates that the benchmark input data should be converted to Parquet and the Hive \
      tables reconfigured accordingly.")
  parser.add_argument(
    "--skip-parquet-conversion",
    action="store_true",
    default=False,
    dest="skip_parquet_conversion",
    help="Indicates that the benchmark input data has already been converted to Parquet. This only \
      makes sense if the \"--parquet\" option is also specified.")
  parser.add_argument(
    "-c",
    "--compress-output",
    action="store_true",
    default=False,
    dest="compress_output",
    help="Whether to compress output tables.")
  parser.add_argument(
    "-n",
    "--num-trials",
    dest="num_trials",
    help="The number of times to execute each query.",
    metavar="NUM-TRIALS",
    required=True,
    type=int)
  parser.add_argument(
    "-q",
    "--query",
    action="append",
    choices=["1a", "1b", "1c", "2a", "2b", "2c", "3a", "3b", "3c", "4"],
    dest="queries",
    help="Which Big Data Benchmark query to execute. This may be specified multiple times to \
      execute multiple queries.",
    metavar="QUERY",
    required=True)
  parser.add_argument(
    "-g",
    "--git-branch",
    action="append",
    dest="branches",
    help="Which Git branch to use. This may be specified multiple times to test multiple branches.",
    metavar="GIT-BRANCH",
    required=True)

  args = parser.parse_args()
  if (args.skip_parquet_conversion and not args.parquet):
    print "\"--skip-parquet-conversion\" only makes sense if \"--parquet\" is also specified. \
      Exiting..."
    System.exit(1)
  return args

def execute_queries_for_branch(aws_key_id, aws_key, args, branch, is_first_branch):
  """
  Checks out the specified branch and runs the provided queries. Copies the event log and the
  specified executor's continuous monitor to `args.output_dir`.

  `is_first_branch` should be set to True if this is the first branch to be tested.
  """
  print_heading("Testing branch \"%s\"" % branch)
  utils.ssh_call(args.driver_addr, "cd /root/spark/; git checkout %s" % branch, args.identity_file)

  if (args.jar_dir is not None):
    print "Retrieving JAR"
    copy_command = "cp -v %s/%s/* /root/spark/assembly/target/scala-2.10/ " % (args.jar_dir, branch)

    # TODO: This will cause an error if the file specified by jar_filename does not exist. We should
    #       check if the file exists before trying to copy it.
    utils.ssh_call(args.driver_addr, copy_command, args.identity_file)
  else:
    print "Compiling spark-monotasks"
    compile_command = "cd /root/spark; \
      build/mvn -Dhadoop.version=2.0.0-cdh4.2.0 -Phive -Phive-thriftserver -DskipTests -e package"
    utils.ssh_call(args.driver_addr, compile_command, args.identity_file)

  print "Copying spark-monotasks to slaves"
  utils.ssh_call(
    args.driver_addr, "/root/spark-ec2/copy-dir --delete /root/spark", args.identity_file)

  do_prepare = is_first_branch
  for query in args.queries:
    execute_query(aws_key_id, aws_key, args, query, branch, do_prepare)
    # Only do the prepare step if this is the first query of the first branch.
    do_prepare = False

def execute_query(aws_key_id, aws_key, args, query, branch, do_prepare):
  """
  Executes the specified query using the branch that is currently checked out (whose name is
  `branch`). Copies the event log and the specified executor's continuous monitor file to
  `args.output_dir`.

  `do_prepare` should be set to True if the Hive tables for the benchmark data need to be
  regenerated or if the benchmark data needs to be converted to Parquet.
  """
  print_heading("Executing %s %s of query %s using branch \"%s\"" %
    (args.num_trials, "trial" if (args.num_trials == 1) else "trials", query, branch))

  # Restart Spark and the Thrift server in order to make sure that we are using the correct version,
  # and to start using new log files.
  stop_thriftserver(args.driver_addr, args.identity_file)
  stop_spark(args.driver_addr, args.identity_file)
  start_spark(args.driver_addr, args.identity_file)

  benchmark_runner_dir = os.path.join(args.benchmark_dir, "runner")
  if (do_prepare):
    print "Preparing benchmark data"
    prepare_benchmark_script = os.path.join(benchmark_runner_dir, "prepare-benchmark.sh")
    prepare_benchmark_command = "%s \
      --spark \
      --aws-key-id=%s \
      --aws-key=%s \
      --spark-host=%s \
      --spark-identity-file=%s \
      --scale-factor=%s \
      --file-format=%s \
      --skip-s3-import" % \
      (prepare_benchmark_script,
        aws_key_id,
        aws_key,
        args.driver_addr,
        args.identity_file,
        args.scale_factor,
        args.file_format)
    if (args.parquet):
      prepare_benchmark_command += " --parquet"
      if (args.skip_parquet_conversion):
        prepare_benchmark_command += " --skip-parquet-conversion"
    execute_shell_command(prepare_benchmark_command)
  else:
    start_thriftserver(args.driver_addr, args.identity_file)

  print "Executing query"
  run_query_script = os.path.join(benchmark_runner_dir, "run-query.sh")
  run_query_command = "%s \
    --spark \
    --spark-host=%s \
    --spark-identity-file=%s \
    --query-num=%s \
    --num-trials=%s \
    --spark-no-cache \
    --clear-buffer-cache " % \
    (run_query_script, args.driver_addr, args.identity_file, query, args.num_trials)
  if (args.compress_output):
    run_query_command += " --compress"
  execute_shell_command(run_query_command)

  # Stop the Thrift server and Spark in order to stop using the Spark log files.
  stop_thriftserver(args.driver_addr, args.identity_file)
  stop_spark(args.driver_addr, args.identity_file)

  print "Retrieving logs"
  copy_logs_script = os.path.join(args.scripts_dir, "copy_logs.py")
  output_file_prefix = "%s_%s" % (query, branch)
  copy_logs_command = "python2 %s \
    --driver-host=%s \
    --executor-host=%s \
    --username=root \
    --identity-file=%s \
    --filename-prefix=%s" % \
    (copy_logs_script, args.driver_addr, args.executor_addr, args.identity_file, output_file_prefix)
  execute_shell_command(copy_logs_command)

  # Move the logs into a new directory: output_dir/query/branch/
  experiment_output_dir = os.path.join(args.output_dir, query, branch)
  execute_shell_command("mkdir -pv %s" % experiment_output_dir)
  execute_shell_command("mv -v %s_* %s/" % (output_file_prefix, experiment_output_dir))

def get_env_vars(keys):
  """
  Retrieves the values of the environment variables in `keys`. Exits with an error code of 1 if any
  of the requested  environment variable is not set.
  """
  def get_env_var(key):
    value = os.environ.get(key)
    if (value is None):
      print "Please set the environment variable \"%s\". Exiting..." % key
      exit(1)
    return value

  return map(lambda key: get_env_var(key), keys)

def execute_shell_command(command):
  print command
  subprocess.check_call(command, shell=True)

def start_spark(driver_addr, identity_file):
  print "Starting Spark"
  utils.ssh_call(driver_addr, "/root/spark/sbin/start-all.sh", identity_file=identity_file)

def stop_spark(driver_addr, identity_file):
  print "Stopping Spark"
  utils.ssh_call(driver_addr, "/root/spark/sbin/stop-all.sh", identity_file=identity_file)

def stop_thriftserver(driver_addr, identity_file):
  print "Stopping the Thrift server"
  utils.ssh_call(driver_addr, "/root/spark/sbin/stop-thriftserver.sh", identity_file=identity_file)

def start_thriftserver(driver_addr, identity_file):
  print "Starting the Thrift server"
  # The value for "--executor-memory" is taken from the default value for the "--executor-memory"
  # parameter in the benchmark/runner/prepare_benchmark.py script.
  utils.ssh_call(
    driver_addr,
    "/root/spark/sbin/start-thriftserver.sh --executor-memory 24G",
    identity_file=identity_file)
  # TODO: We should keep checking to see if the JDBC server has started yet.
  print "Sleeping for 30 seconds so the JDBC server can start"
  time.sleep(30)

def print_heading(heading):
  def print_line(middle):
    print "#%s#" % middle

  bar = ("=" * (len(heading) + 2))

  print "\n\n"
  print_line(bar)
  print_line(" %s " % heading)
  print_line(bar)
  print "\n\n"

def print_info(args):
  print_heading("Running Big Data Benchmark experiments")
  print "Queries:"
  for query in args.queries:
    print "\t%s" % query

  print "Branches:"
  for branch in args.branches:
    print "\t%s" % branch

  print "Scale factor: %s" % args.scale_factor
  print "Number of trials: %s\n\n" % args.num_trials

if __name__ == "__main__":
  main(sys.argv[1:])
