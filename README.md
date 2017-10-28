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
