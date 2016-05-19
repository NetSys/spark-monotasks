"""
This script runs the AMPLab Big Data Benchmark on an EC2 cluster whose master it is executing on.
It assumes that the benchmark data has already been loaded onto the cluster.

Run this script with the "-h" flag for usage information.
"""

import argparse
import os
from os import path
import subprocess

import utils

SPARK_DIR = utils.get_full_path("spark")
SPARK_SBIN_DIR = path.join(SPARK_DIR, "sbin")


def main():
  aws_key_id, aws_key = get_env_vars(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])
  args = parse_args()
  print_info(args)

  is_first_branch = True
  for branch in args.branches:
    execute_queries_for_branch(aws_key_id, aws_key, args, branch, is_first_branch)
    is_first_branch = False
  print_heading("Done!")


def parse_args():
  parser = argparse.ArgumentParser(description="Runs Big Data Benchmark experiments on EC2.")
  parser.add_argument(
    "-b",
    "--benchmark-dir",
    help="The local path to the \"benchmark\" repository.",
    required=True)
  parser.add_argument(
    "-i",
    "--identity-file",
    help="The cluster's identity file.",
    required=True)
  parser.add_argument(
    "-j",
    "--jar-dir",
    help="The local directory where pre-compiled JARs are stored. Not specifying this option \
      leads to recompilation. The JAR for a branch named 'branch' should be stored in the \
      directory %(metavar)s/branch/")
  parser.add_argument(
    "-o",
    "--output-dir",
    help="The local directory in which the resulting log files will be stored. Log files for a \
      particular combination of query and branch are stored in the directory \
      %(metavar)s/query/branch/",
    required=True)
  parser.add_argument(
    "-s",
    "--scale-factor",
    help="Which Big Data Benchmark scale factor to use.",
    required=True,
    type=int)
  parser.add_argument(
    "-f",
    "--file-format",
    choices=["sequence", "sequence-snappy", "text", "text-deflate"],
    help="The file format of the input data.",
    required=True)
  parser.add_argument(
    "-p",
    "--parquet",
    action="store_true",
    default=False,
    help="Indicates that the benchmark input data should be converted to Parquet and the Hive \
      tables reconfigured accordingly.")
  parser.add_argument(
    "--skip-parquet-conversion",
    action="store_true",
    default=False,
    help="Indicates that the benchmark input data has already been converted to Parquet. This only \
      makes sense if the '--parquet' option is also specified.")
  parser.add_argument(
    "-c",
    "--compress-output",
    action="store_true",
    default=False,
    help="Whether to compress output tables.")
  parser.add_argument(
    "--memory",
    action="store_true",
    default=False,
    help=("If specified, then the input and output tables will be stored in memory. This " +
      "automatically caches the input tables in memory before running a query."))
  parser.add_argument(
    "-n",
    "--num-trials",
    help="The number of times to execute each query.",
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
    required=True)
  parser.add_argument(
    "-g",
    "--git-branch",
    action="append",
    dest="branches",
    help="Which Git branch to use. This may be specified multiple times to test multiple branches.",
    required=True)

  args = parser.parse_args()
  assert (not args.skip_parquet_conversion) or args.parquet, \
    "'--skip-parquet-conversion' only makes sense if '--parquet' is also specified."
  return args


def execute_queries_for_branch(aws_key_id, aws_key, args, branch, is_first_branch):
  """
  Checks out the specified branch and runs the provided queries. Copies the event log and
  continuous monitors to `args.output_dir`.

  `is_first_branch` should be set to True if this is the first branch to be tested.
  """
  print_heading("Testing branch '{}'".format(branch))
  execute_shell_command("cd {}; git checkout {}".format(SPARK_DIR, branch))

  jar_dir = args.jar_dir
  if jar_dir is None:
    print "Compiling spark-monotasks"
    mvn_filepath = path.join("build", "mvn")
    execute_shell_command("cd {}; {} ".format(SPARK_DIR, mvn_filepath) +
      "-Dhadoop.version=2.0.0-cdh4.2.0 -Phive -Phive-thriftserver -DskipTests -e clean package")
  else:
    print "Retrieving JAR"
    jar_filepath = path.join(jar_dir, branch, "*")
    jar_dest_dir = path.join(SPARK_DIR, "assembly", "target", "scala-2.10")

    # TODO: This will cause an error if the file specified by jar_filename does not exist. We should
    #       check if the file exists before trying to copy it.
    execute_shell_command("cp -v {} {} ".format(jar_filepath, jar_dest_dir))

  print "Copying spark-monotasks to slaves"
  copy_dir_filepath = utils.get_full_path(path.join("spark-ec2", "copy-dir"))
  execute_shell_command("{} --delete {}".format(copy_dir_filepath, SPARK_DIR))

  for query in args.queries:
    execute_query(aws_key_id, aws_key, args, query, branch, is_first_branch)

def execute_query(aws_key_id, aws_key, args, query, branch, is_first_branch):
  """
  Executes the specified query using the branch that is currently checked out (whose name is
  `branch`). Copies the event log and continuous monitors file to  `args.output_dir`.

  `is_first_branch` should be set to True if this is the first branch to be tested.
  """
  print_heading("Executing {} {} of query {} using branch '{}'".format(
    args.num_trials, "trial" if (args.num_trials == 1) else "trials", query, branch))

  # Restart Spark and the Thrift server in order to make sure that we are using the correct version,
  # and to start using new log files.
  stop_thriftserver()
  stop_spark()
  start_spark()

  benchmark_runner_dir = path.join(args.benchmark_dir, "runner")
  driver_addr = subprocess.check_output(
    "curl -s http://169.254.169.254/latest/meta-data/public-hostname", shell=True)

  print "Creating benchmark tables and starting the Thrift server"
  prepare_benchmark_script = path.join(benchmark_runner_dir, "prepare-benchmark.sh")
  prepare_benchmark_command = "{} \
    --spark \
    --aws-key-id={} \
    --aws-key={} \
    --spark-host={} \
    --spark-identity-file={} \
    --scale-factor={} \
    --file-format={} \
    --skip-s3-import".format(
      prepare_benchmark_script,
      aws_key_id,
      aws_key,
      driver_addr,
      args.identity_file,
      args.scale_factor,
      args.file_format)

  if args.parquet:
    prepare_benchmark_command += " --parquet"
    if not is_first_branch or args.skip_parquet_conversion:
      prepare_benchmark_command += " --skip-parquet-conversion"
  execute_shell_command(prepare_benchmark_command)

  if args.memory:
    cache_table_for_query(query)

  print "Executing query"
  run_query_script = path.join(benchmark_runner_dir, "run-query.sh")
  run_query_command = "{} \
    --spark \
    --spark-host={} \
    --spark-identity-file={} \
    --query-num={} \
    --num-trials={} \
    --clear-buffer-cache".format(
      run_query_script,
      driver_addr,
      args.identity_file,
      query,
      args.num_trials)
  if args.compress_output:
    run_query_command += " --compress"
  if args.memory:
    run_query_command += " --spark-cache-output-tables"
  execute_shell_command(run_query_command)

  # Stop the Thrift server and Spark in order to stop using the Spark log files.
  stop_thriftserver()
  stop_spark()

  print "Retrieving logs"
  parameters = [query, branch]
  log_dir = utils.copy_all_logs(parameters, utils.get_workers())
  log_files = path.join(log_dir, "*")

  # Move the logs into a new directory: output_dir/query/branch/
  output_dir = path.join(args.output_dir, query, branch)
  execute_shell_command("mkdir -pv {}".format(output_dir))
  execute_shell_command("mv -v {} {}".format(log_files, output_dir))
  execute_shell_command("rm -rf {}".format(log_dir))


def get_env_vars(keys):
  """ Retrieves the values of the environment variables in `keys`. """
  def get_env_var(key):
    value = os.environ.get(key)
    assert value is not None, "Please set the environment variable '{}'.".format(key)
    return value

  return map(lambda key: get_env_var(key), keys)


def execute_shell_command(command):
  print command
  subprocess.check_call(command, shell=True)


def start_spark():
  print "Starting Spark"
  execute_shell_command(path.join(SPARK_SBIN_DIR, "start-all.sh"))


def stop_spark():
  print "Stopping Spark"
  execute_shell_command(path.join(SPARK_SBIN_DIR, "stop-all.sh"))


def stop_thriftserver():
  print "Stopping the Thrift server"
  execute_shell_command(path.join(SPARK_SBIN_DIR, "stop-thriftserver.sh"))


def cache_table_for_query(query):
  """
  Caches the tables required by the specified query in memory. Spark and the Thrift server must be
  running before calling this function.
  """
  if "4" in query:
    cache_table(query, table="documents")
  else:
    if "1" not in query:
      cache_table(query, table="uservisits")
    if "2" not in query:
      cache_table(query, table="rankings")


def cache_table(query, table):
  """
  Caches the specified table in memory. Spark and the Thrift server must be running before calling
  this function.
  """
  print "Caching table '{}' for query {}...".format(table, query)
  execute_shell_command("/root/spark/bin/beeline -u jdbc:hive2://localhost:10000 -n root " +
    "-e 'CACHE TABLE {}'".format(table))


def print_heading(heading):
  def print_line(middle):
    print "#{}#".format(middle)

  bar = ("=" * (len(heading) + 2))

  print "\n\n"
  print_line(bar)
  print_line(" {} ".format(heading))
  print_line(bar)
  print "\n\n"


def print_info(args):
  print_heading("Running Big Data Benchmark experiments")
  print "Queries:"
  for query in args.queries:
    print "\t{}".format(query)

  print "Branches:"
  for branch in args.branches:
    print "\t{}".format(branch)

  print "Scale factor: {}".format(args.scale_factor)
  print "Number of trials: {}\n\n".format(args.num_trials)


if __name__ == "__main__":
  main()
