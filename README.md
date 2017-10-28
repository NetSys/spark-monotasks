# Monotasks for Apache Spark

This repository contains the source code for a modified version of Apache Spark
that decomposes jobs into monotasks rather than regular, multi-resource tasks.
For more information about monotasks, refer to the research paper.

This code is not suitable for production use, and does not implement a number of
features that are available in Apache Spark.  It is based on version 1.3 of Apache
Spark. For more information about Apache Spark, refer to the
[Spark web page](http://spark.apache.org).

## Running research experiments

The code in this repository is not supported, so run it at your own risk! If you
are interested in running research experiments to compare performance of monotasks
to performance of Spark, this section gives a brief overview of how to do so for
one benchmark workload: the big data benchmark.

1. Launch a cluster using the [Monotasks branch of the spark-ec2](https://github.com/kayousterhout/spark-ec2/tree/monotasks) scripts.  Using 5 m2.4xlarge instances is recommended.

   ```console
   $ ./spark-ec2 launch monotasks -t m2.4xlarge -s 5 --spot-price 3.00 -i <your keypair name>.pem -k <your keypair>
   ```

   The keypair must be registered with Amazon EC2 in the us-west-2  region (or you can specify a different region with the `-r` flag), and you'll need to have your AWS credentials in environment variables (for more about this, refer to the `spark-ec2` documentation).  The most important thing is the spot-price: beware that this particular command *could* lead to you getting charged as much as $3.00 / hour / instance (but in general will not have a price nearly that high, because Amazon uses a 2nd price auction).

2. Log in to the machines:
   
   ```console
   $ ./spark-ec2 login monotasks -i <your keypair name>.pem
   ```
  
  Build the appropriate monotasks branch of Spark using the the experiment_scripts/build.sh script (this will compile and copy the new code to the cluster). I would recommend doing this (and all of the subsequent steps) in a screen session so that, if you get logged out, you can re-attach to the same session.
By default, the monotasks master branch will be built by the script.
To build a version of Spark to compare against, build the `spark_with_logging` branch, which is a version of Spark that has some additional logging so that output can be parsed by the monotasks parsing scripts.

3. Configure Spark.  In particular, I recommend reducing the memory allocated to Spark by roughly 15% (e.g., from 58315m to 48315m) (which is specified in `spark/conf/spark-defaults.conf`) to avoid issues with the buffer cache, and turning the log level (specified in `spark/conf/log4j.properties`) down to WARN (otherwise the logs can cause the worker machine disks to fill up when running long experiments).

4. Clone the repository with the big data benchmark onto the master on the cluster:

   ```console
   $ git clone -b spark-sql-support https://github.com/kayousterhout/benchmark.git
   ```
  You'll also need to copy your ssh key (that you used to login to the AWS machines) onto the cluster.

5. Use the big data benchmark repo to load the benchmark data:

  ```console
   $ benchmark/runner/prepare-benchmark.sh -s --spark-host <hostname of the master machine> --scale-factor 5 --spark-identity-file key.pem --aws-key-id=<your key id here> --aws-key=<your secret access key here> 
   ```

   This will copy the benchmark data with scale factor 5 from S3 onto the cluster, using Hadoop.  It will take approximately 10 minutes.  Avoid killing or stopping this at all costs.  If it gets stopped midway, be sure to delete all of the data from HDFS before re-starting it, otherwise you’ll end up with incorrectly setup data directories that will lead to lots of problems later.  To delete partially setup data if you need to restart the script, run:
   
   ```console
   /root/ephemeral-hdfs/bin/hadoop dfs -rmr /user/spark/benchmark/*
   ```

6. Save the compiled Jars of both of the Spark branches.  This allows you to tell the benchmark running script to read the jar from there, rather than re-compiling from scratch (which will work, but will take longer).  For example, to save the current compiled jar as the `spark_with_logging` branch:

    ```console
    $ mkdir /root/jars/
    $ mkdir /root/jars/spark_with_logging
    $ cp ../assembly/target/scala-2.10/spark-assembly-1.3.1-SNAPSHOT-hadoop2.0.0-cdh4.2.0.jar /root/spark_with_logging_compiled/
    ```
 
7. Launch the benchmark.  This relies on a script, `run_bdb_experiments.py`, in the `experiment_scripts` directory in this repository.

   ```console
   $ python27 run_bdb_experiments.py \
       --benchmark-dir /root/benchmark/ \
       -i /root/benchmark/runner/key.pem \
       --output-dir /root/spark_bdb_results \
       --scale-factor 5 \
       --file-format sequence-snappy \
       --num-trials 6 \
       -g spark_with_logging --jar-dir /root/jars \
       -q 1a -q 1b -q 1c -q 2a -q 2b -q 2c -q 3a -q 3b -q 3c -q 4
   ```

Generally it’s good practice to dump the first trial, which tends to be slower because the JVM needs to be warmed up.
Note that you'll also need to explicitly specify the Python version (i.e., use `python27` rather than `python`),
because the script relies on
some newer features of Python that are not present in the default version installed on the AMI.
