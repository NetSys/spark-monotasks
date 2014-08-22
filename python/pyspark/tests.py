#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""
from array import array
from fileinput import input
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

from pyspark.context import SparkContext
from pyspark.files import SparkFiles
from pyspark.serializers import read_int
from pyspark.shuffle import Aggregator, InMemoryMerger, ExternalMerger

_have_scipy = False
_have_numpy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass
try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy, but that's okay, we'll skip those tests
    pass


SPARK_HOME = os.environ["SPARK_HOME"]


class TestMerger(unittest.TestCase):

    def setUp(self):
        self.N = 1 << 16
        self.l = [i for i in xrange(self.N)]
        self.data = zip(self.l, self.l)
        self.agg = Aggregator(lambda x: [x],
                              lambda x, y: x.append(y) or x,
                              lambda x, y: x.extend(y) or x)

    def test_in_memory(self):
        m = InMemoryMerger(self.agg)
        m.mergeValues(self.data)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = InMemoryMerger(self.agg)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data))
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

    def test_small_dataset(self):
        m = ExternalMerger(self.agg, 1000)
        m.mergeValues(self.data)
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 1000)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data))
        self.assertEqual(m.spills, 0)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

    def test_medium_dataset(self):
        m = ExternalMerger(self.agg, 10)
        m.mergeValues(self.data)
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)))

        m = ExternalMerger(self.agg, 10)
        m.mergeCombiners(map(lambda (x, y): (x, [y]), self.data * 3))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(sum(v) for k, v in m.iteritems()),
                         sum(xrange(self.N)) * 3)

    def test_huge_dataset(self):
        m = ExternalMerger(self.agg, 10)
        m.mergeCombiners(map(lambda (k, v): (k, [str(v)]), self.data * 10))
        self.assertTrue(m.spills >= 1)
        self.assertEqual(sum(len(v) for k, v in m._recursive_merged_items(0)),
                         self.N * 10)
        m._cleanup()


class SerializationTestCase(unittest.TestCase):

    def test_namedtuple(self):
        from collections import namedtuple
        from cPickle import dumps, loads
        P = namedtuple("P", "x y")
        p1 = P(1, 3)
        p2 = loads(dumps(p1, 2))
        self.assertEquals(p1, p2)


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name, batchSize=2)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path


class TestCheckpoint(PySparkTestCase):

    def setUp(self):
        PySparkTestCase.setUp(self)
        self.checkpointDir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        PySparkTestCase.tearDown(self)
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual("file:" + self.checkpointDir.name,
                         os.path.dirname(os.path.dirname(flatMappedRDD.getCheckpointFile())))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertTrue(flatMappedRDD.getCheckpointFile() is not None)
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile(),
                                            flatMappedRDD._jrdd_deserializer)
        self.assertEquals([1, 2, 3, 4], recovered.collect())


class TestAddFile(PySparkTestCase):

    def test_add_py_file(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this job fails due to `userlibrary` not being on the Python path:
        # disable logging in log4j temporarily
        log4j = self.sc._jvm.org.apache.log4j
        old_level = log4j.LogManager.getRootLogger().getLevel()
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

        def func(x):
            from userlibrary import UserClass
            return UserClass().hello()
        self.assertRaises(Exception,
                          self.sc.parallelize(range(2)).map(func).first)
        log4j.LogManager.getRootLogger().setLevel(old_level)

        # Add the file, so the job should now succeed:
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        res = self.sc.parallelize(range(2)).map(func).first()
        self.assertEqual("Hello World!", res)

    def test_add_file_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        self.sc.addFile(path)
        download_path = SparkFiles.get("hello.txt")
        self.assertNotEqual(path, download_path)
        with open(download_path) as test_file:
            self.assertEquals("Hello World!\n", test_file.readline())

    def test_add_py_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlibrary import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addFile(path)
        from userlibrary import UserClass
        self.assertEqual("Hello World!", UserClass().hello())

    def test_add_egg_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlib import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlib-0.1-py2.7.egg")
        self.sc.addPyFile(path)
        from userlib import UserClass
        self.assertEqual("Hello World from inside a package!", UserClass().hello())


class TestRDDFunctions(PySparkTestCase):

    def test_failed_sparkcontext_creation(self):
        # Regression test for SPARK-1550
        self.sc.stop()
        self.assertRaises(Exception, lambda: SparkContext("an-invalid-master-name"))
        self.sc = SparkContext("local")

    def test_save_as_textfile_with_unicode(self):
        # Regression test for SPARK-970
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = ''.join(input(glob(tempFile.name + "/part-0000*")))
        self.assertEqual(x, unicode(raw_contents.strip(), "utf-8"))

    def test_transforming_cartesian_result(self):
        # Regression test for SPARK-1034
        rdd1 = self.sc.parallelize([1, 2])
        rdd2 = self.sc.parallelize([3, 4])
        cart = rdd1.cartesian(rdd2)
        result = cart.map(lambda (x, y): x + y).collect()

    def test_transforming_pickle_file(self):
        # Regression test for SPARK-2601
        data = self.sc.parallelize(["Hello", "World!"])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsPickleFile(tempFile.name)
        pickled_file = self.sc.pickleFile(tempFile.name)
        pickled_file.map(lambda x: x).collect()

    def test_cartesian_on_textfile(self):
        # Regression test for
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        a = self.sc.textFile(path)
        result = a.cartesian(a).collect()
        (x, y) = result[0]
        self.assertEqual("Hello World!", x.strip())
        self.assertEqual("Hello World!", y.strip())

    def test_deleting_input_files(self):
        # Regression test for SPARK-1025
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write("Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        self.assertRaises(Exception, lambda: filtered_data.count())

    def testAggregateByKey(self):
        data = self.sc.parallelize([(1, 1), (1, 1), (3, 2), (5, 1), (5, 3)], 2)

        def seqOp(x, y):
            x.add(y)
            return x

        def combOp(x, y):
            x |= y
            return x

        sets = dict(data.aggregateByKey(set(), seqOp, combOp).collect())
        self.assertEqual(3, len(sets))
        self.assertEqual(set([1]), sets[1])
        self.assertEqual(set([2]), sets[3])
        self.assertEqual(set([1, 3]), sets[5])

    def test_itemgetter(self):
        rdd = self.sc.parallelize([range(10)])
        from operator import itemgetter
        self.assertEqual([1], rdd.map(itemgetter(1)).collect())
        self.assertEqual([(2, 3)], rdd.map(itemgetter(2, 3)).collect())

    def test_namedtuple_in_rdd(self):
        from collections import namedtuple
        Person = namedtuple("Person", "id firstName lastName")
        jon = Person(1, "Jon", "Doe")
        jane = Person(2, "Jane", "Doe")
        theDoes = self.sc.parallelize([jon, jane])
        self.assertEquals([jon, jane], theDoes.collect())


class TestIO(PySparkTestCase):

    def test_stdout_redirection(self):
        import subprocess

        def func(x):
            subprocess.check_call('ls', shell=True)
        self.sc.parallelize([1]).foreach(func)


class TestInputFormat(PySparkTestCase):

    def setUp(self):
        PySparkTestCase.setUp(self)
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)
        self.sc._jvm.WriteInputFormatTestDataGenerator.generateData(self.tempdir.name, self.sc._jsc)

    def tearDown(self):
        PySparkTestCase.tearDown(self)
        shutil.rmtree(self.tempdir.name)

    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfint/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        doubles = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfdouble/",
                                              "org.apache.hadoop.io.DoubleWritable",
                                              "org.apache.hadoop.io.Text").collect())
        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.assertEqual(doubles, ed)

        bytes = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbytes/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BytesWritable").collect())
        ebs = [(1, bytearray('aa', 'utf-8')),
               (1, bytearray('aa', 'utf-8')),
               (2, bytearray('aa', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (2, bytearray('bb', 'utf-8')),
               (3, bytearray('cc', 'utf-8'))]
        self.assertEqual(bytes, ebs)

        text = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sftext/",
                                           "org.apache.hadoop.io.Text",
                                           "org.apache.hadoop.io.Text").collect())
        et = [(u'1', u'aa'),
              (u'1', u'aa'),
              (u'2', u'aa'),
              (u'2', u'bb'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.assertEqual(text, et)

        bools = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfbool/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.assertEqual(bools, eb)

        nulls = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfnull/",
                                            "org.apache.hadoop.io.IntWritable",
                                            "org.apache.hadoop.io.BooleanWritable").collect())
        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.assertEqual(nulls, en)

        maps = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfmap/",
                                           "org.apache.hadoop.io.IntWritable",
                                           "org.apache.hadoop.io.MapWritable").collect())
        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.assertEqual(maps, em)

        # arrays get pickled to tuples by default
        tuples = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable").collect())
        et = [(1, ()),
              (2, (3.0, 4.0, 5.0)),
              (3, (4.0, 5.0, 6.0))]
        self.assertEqual(tuples, et)

        # with custom converters, primitive arrays can stay as arrays
        arrays = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfarray/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        ea = [(1, array('d')),
              (2, array('d', [3.0, 4.0, 5.0])),
              (3, array('d', [4.0, 5.0, 6.0]))]
        self.assertEqual(arrays, ea)

        clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                            "org.apache.hadoop.io.Text",
                                            "org.apache.spark.api.python.TestWritable").collect())
        ec = (u'1',
              {u'__class__': u'org.apache.spark.api.python.TestWritable',
               u'double': 54.0, u'int': 123, u'str': u'test1'})
        self.assertEqual(clazz[0], ec)

        unbatched_clazz = sorted(self.sc.sequenceFile(basepath + "/sftestdata/sfclass/",
                                                      "org.apache.hadoop.io.Text",
                                                      "org.apache.spark.api.python.TestWritable",
                                                      batchSize=1).collect())
        self.assertEqual(unbatched_clazz[0], ec)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.hadoopFile(basepath + "/sftestdata/sfint/",
                                         "org.apache.hadoop.mapred.SequenceFileInputFormat",
                                         "org.apache.hadoop.io.IntWritable",
                                         "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        oldconf = {"mapred.input.dir": hellopath}
        hello = self.sc.hadoopRDD("org.apache.hadoop.mapred.TextInputFormat",
                                  "org.apache.hadoop.io.LongWritable",
                                  "org.apache.hadoop.io.Text",
                                  conf=oldconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        ints = sorted(self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text").collect())
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.assertEqual(ints, ei)

        hellopath = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        newconf = {"mapred.input.dir": hellopath}
        hello = self.sc.newAPIHadoopRDD("org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                        "org.apache.hadoop.io.LongWritable",
                                        "org.apache.hadoop.io.Text",
                                        conf=newconf).collect()
        result = [(0, u'Hello World!')]
        self.assertEqual(hello, result)

    def test_newolderror(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        self.assertRaises(Exception, lambda: self.sc.sequenceFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.io.NotValidWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.hadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapred.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))
        self.assertRaises(Exception, lambda: self.sc.newAPIHadoopFile(
            basepath + "/sftestdata/sfint/",
            "org.apache.hadoop.mapreduce.lib.input.NotValidInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        maps = sorted(self.sc.sequenceFile(
            basepath + "/sftestdata/sfmap/",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            keyConverter="org.apache.spark.api.python.TestInputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestInputValueConverter").collect())
        em = [(u'\x01', []),
              (u'\x01', [3.0]),
              (u'\x02', [1.0]),
              (u'\x02', [1.0]),
              (u'\x03', [2.0])]
        self.assertEqual(maps, em)


class TestOutputFormat(PySparkTestCase):

    def setUp(self):
        PySparkTestCase.setUp(self)
        self.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.tempdir.name)

    def tearDown(self):
        PySparkTestCase.tearDown(self)
        shutil.rmtree(self.tempdir.name, ignore_errors=True)

    def test_sequencefiles(self):
        basepath = self.tempdir.name
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.sc.parallelize(ei).saveAsSequenceFile(basepath + "/sfint/")
        ints = sorted(self.sc.sequenceFile(basepath + "/sfint/").collect())
        self.assertEqual(ints, ei)

        ed = [(1.0, u'aa'), (1.0, u'aa'), (2.0, u'aa'), (2.0, u'bb'), (2.0, u'bb'), (3.0, u'cc')]
        self.sc.parallelize(ed).saveAsSequenceFile(basepath + "/sfdouble/")
        doubles = sorted(self.sc.sequenceFile(basepath + "/sfdouble/").collect())
        self.assertEqual(doubles, ed)

        ebs = [(1, bytearray(b'\x00\x07spam\x08')), (2, bytearray(b'\x00\x07spam\x08'))]
        self.sc.parallelize(ebs).saveAsSequenceFile(basepath + "/sfbytes/")
        bytes = sorted(self.sc.sequenceFile(basepath + "/sfbytes/").collect())
        self.assertEqual(bytes, ebs)

        et = [(u'1', u'aa'),
              (u'2', u'bb'),
              (u'3', u'cc')]
        self.sc.parallelize(et).saveAsSequenceFile(basepath + "/sftext/")
        text = sorted(self.sc.sequenceFile(basepath + "/sftext/").collect())
        self.assertEqual(text, et)

        eb = [(1, False), (1, True), (2, False), (2, False), (2, True), (3, True)]
        self.sc.parallelize(eb).saveAsSequenceFile(basepath + "/sfbool/")
        bools = sorted(self.sc.sequenceFile(basepath + "/sfbool/").collect())
        self.assertEqual(bools, eb)

        en = [(1, None), (1, None), (2, None), (2, None), (2, None), (3, None)]
        self.sc.parallelize(en).saveAsSequenceFile(basepath + "/sfnull/")
        nulls = sorted(self.sc.sequenceFile(basepath + "/sfnull/").collect())
        self.assertEqual(nulls, en)

        em = [(1, {}),
              (1, {3.0: u'bb'}),
              (2, {1.0: u'aa'}),
              (2, {1.0: u'cc'}),
              (3, {2.0: u'dd'})]
        self.sc.parallelize(em).saveAsSequenceFile(basepath + "/sfmap/")
        maps = sorted(self.sc.sequenceFile(basepath + "/sfmap/").collect())
        self.assertEqual(maps, em)

    def test_oldhadoop(self):
        basepath = self.tempdir.name
        dict_data = [(1, {}),
                     (1, {"row1": 1.0}),
                     (2, {"row2": 2.0})]
        self.sc.parallelize(dict_data).saveAsHadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable")
        result = sorted(self.sc.hadoopFile(
            basepath + "/oldhadoop/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable").collect())
        self.assertEqual(result, dict_data)

        conf = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.hadoop.io.MapWritable",
            "mapred.output.dir": basepath + "/olddataset/"
        }
        self.sc.parallelize(dict_data).saveAsHadoopDataset(conf)
        input_conf = {"mapred.input.dir": basepath + "/olddataset/"}
        old_dataset = sorted(self.sc.hadoopRDD(
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.MapWritable",
            conf=input_conf).collect())
        self.assertEqual(old_dataset, dict_data)

    def test_newhadoop(self):
        basepath = self.tempdir.name
        # use custom ArrayWritable types and converters to handle arrays
        array_data = [(1, array('d')),
                      (1, array('d', [1.0, 2.0, 3.0])),
                      (2, array('d', [3.0, 4.0, 5.0]))]
        self.sc.parallelize(array_data).saveAsNewAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        result = sorted(self.sc.newAPIHadoopFile(
            basepath + "/newhadoop/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter").collect())
        self.assertEqual(result, array_data)

        conf = {
            "mapreduce.outputformat.class":
                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.spark.api.python.DoubleArrayWritable",
            "mapred.output.dir": basepath + "/newdataset/"
        }
        self.sc.parallelize(array_data).saveAsNewAPIHadoopDataset(
            conf,
            valueConverter="org.apache.spark.api.python.DoubleArrayToWritableConverter")
        input_conf = {"mapred.input.dir": basepath + "/newdataset/"}
        new_dataset = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.spark.api.python.DoubleArrayWritable",
            valueConverter="org.apache.spark.api.python.WritableToDoubleArrayConverter",
            conf=input_conf).collect())
        self.assertEqual(new_dataset, array_data)

    def test_newolderror(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/newolderror/saveAsHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/newolderror/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat"))

    def test_bad_inputs(self):
        basepath = self.tempdir.name
        rdd = self.sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
        self.assertRaises(Exception, lambda: rdd.saveAsHadoopFile(
            basepath + "/badinputs/saveAsHadoopFile/",
            "org.apache.hadoop.mapred.NotValidOutputFormat"))
        self.assertRaises(Exception, lambda: rdd.saveAsNewAPIHadoopFile(
            basepath + "/badinputs/saveAsNewAPIHadoopFile/",
            "org.apache.hadoop.mapreduce.lib.output.NotValidOutputFormat"))

    def test_converters(self):
        # use of custom converters
        basepath = self.tempdir.name
        data = [(1, {3.0: u'bb'}),
                (2, {1.0: u'aa'}),
                (3, {2.0: u'dd'})]
        self.sc.parallelize(data).saveAsNewAPIHadoopFile(
            basepath + "/converters/",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
            keyConverter="org.apache.spark.api.python.TestOutputKeyConverter",
            valueConverter="org.apache.spark.api.python.TestOutputValueConverter")
        converted = sorted(self.sc.sequenceFile(basepath + "/converters/").collect())
        expected = [(u'1', 3.0),
                    (u'2', 1.0),
                    (u'3', 2.0)]
        self.assertEqual(converted, expected)

    def test_reserialization(self):
        basepath = self.tempdir.name
        x = range(1, 5)
        y = range(1001, 1005)
        data = zip(x, y)
        rdd = self.sc.parallelize(x).zip(self.sc.parallelize(y))
        rdd.saveAsSequenceFile(basepath + "/reserialize/sequence")
        result1 = sorted(self.sc.sequenceFile(basepath + "/reserialize/sequence").collect())
        self.assertEqual(result1, data)

        rdd.saveAsHadoopFile(
            basepath + "/reserialize/hadoop",
            "org.apache.hadoop.mapred.SequenceFileOutputFormat")
        result2 = sorted(self.sc.sequenceFile(basepath + "/reserialize/hadoop").collect())
        self.assertEqual(result2, data)

        rdd.saveAsNewAPIHadoopFile(
            basepath + "/reserialize/newhadoop",
            "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
        result3 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newhadoop").collect())
        self.assertEqual(result3, data)

        conf4 = {
            "mapred.output.format.class": "org.apache.hadoop.mapred.SequenceFileOutputFormat",
            "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.value.class": "org.apache.hadoop.io.IntWritable",
            "mapred.output.dir": basepath + "/reserialize/dataset"}
        rdd.saveAsHadoopDataset(conf4)
        result4 = sorted(self.sc.sequenceFile(basepath + "/reserialize/dataset").collect())
        self.assertEqual(result4, data)

        conf5 = {"mapreduce.outputformat.class":
                 "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",
                 "mapred.output.key.class": "org.apache.hadoop.io.IntWritable",
                 "mapred.output.value.class": "org.apache.hadoop.io.IntWritable",
                 "mapred.output.dir": basepath + "/reserialize/newdataset"}
        rdd.saveAsNewAPIHadoopDataset(conf5)
        result5 = sorted(self.sc.sequenceFile(basepath + "/reserialize/newdataset").collect())
        self.assertEqual(result5, data)

    def test_unbatched_save_and_read(self):
        basepath = self.tempdir.name
        ei = [(1, u'aa'), (1, u'aa'), (2, u'aa'), (2, u'bb'), (2, u'bb'), (3, u'cc')]
        self.sc.parallelize(ei, numSlices=len(ei)).saveAsSequenceFile(
            basepath + "/unbatched/")

        unbatched_sequence = sorted(self.sc.sequenceFile(
            basepath + "/unbatched/",
            batchSize=1).collect())
        self.assertEqual(unbatched_sequence, ei)

        unbatched_hadoopFile = sorted(self.sc.hadoopFile(
            basepath + "/unbatched/",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            batchSize=1).collect())
        self.assertEqual(unbatched_hadoopFile, ei)

        unbatched_newAPIHadoopFile = sorted(self.sc.newAPIHadoopFile(
            basepath + "/unbatched/",
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            batchSize=1).collect())
        self.assertEqual(unbatched_newAPIHadoopFile, ei)

        oldconf = {"mapred.input.dir": basepath + "/unbatched/"}
        unbatched_hadoopRDD = sorted(self.sc.hadoopRDD(
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            conf=oldconf,
            batchSize=1).collect())
        self.assertEqual(unbatched_hadoopRDD, ei)

        newconf = {"mapred.input.dir": basepath + "/unbatched/"}
        unbatched_newAPIHadoopRDD = sorted(self.sc.newAPIHadoopRDD(
            "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat",
            "org.apache.hadoop.io.IntWritable",
            "org.apache.hadoop.io.Text",
            conf=newconf,
            batchSize=1).collect())
        self.assertEqual(unbatched_newAPIHadoopRDD, ei)

    def test_malformed_RDD(self):
        basepath = self.tempdir.name
        # non-batch-serialized RDD[[(K, V)]] should be rejected
        data = [[(1, "a")], [(2, "aa")], [(3, "aaa")]]
        rdd = self.sc.parallelize(data, numSlices=len(data))
        self.assertRaises(Exception, lambda: rdd.saveAsSequenceFile(
            basepath + "/malformed/sequence"))


class TestDaemon(unittest.TestCase):

    def connect(self, port):
        from socket import socket, AF_INET, SOCK_STREAM
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(('127.0.0.1', port))
        # send a split index of -1 to shutdown the worker
        sock.send("\xFF\xFF\xFF\xFF")
        sock.close()
        return True

    def do_termination_test(self, terminator):
        from subprocess import Popen, PIPE
        from errno import ECONNREFUSED

        # start daemon
        daemon_path = os.path.join(os.path.dirname(__file__), "daemon.py")
        daemon = Popen([sys.executable, daemon_path], stdin=PIPE, stdout=PIPE)

        # read the port number
        port = read_int(daemon.stdout)

        # daemon should accept connections
        self.assertTrue(self.connect(port))

        # request shutdown
        terminator(daemon)
        time.sleep(1)

        # daemon should no longer accept connections
        try:
            self.connect(port)
        except EnvironmentError as exception:
            self.assertEqual(exception.errno, ECONNREFUSED)
        else:
            self.fail("Expected EnvironmentError to be raised")

    def test_termination_stdin(self):
        """Ensure that daemon and workers terminate when stdin is closed."""
        self.do_termination_test(lambda daemon: daemon.stdin.close())

    def test_termination_sigterm(self):
        """Ensure that daemon and workers terminate on SIGTERM."""
        from signal import SIGTERM
        self.do_termination_test(lambda daemon: os.kill(daemon.pid, SIGTERM))


class TestWorker(PySparkTestCase):

    def test_cancel_task(self):
        temp = tempfile.NamedTemporaryFile(delete=True)
        temp.close()
        path = temp.name

        def sleep(x):
            import os
            import time
            with open(path, 'w') as f:
                f.write("%d %d" % (os.getppid(), os.getpid()))
            time.sleep(100)

        # start job in background thread
        def run():
            self.sc.parallelize(range(1)).foreach(sleep)
        import threading
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()

        daemon_pid, worker_pid = 0, 0
        while True:
            if os.path.exists(path):
                data = open(path).read().split(' ')
                daemon_pid, worker_pid = map(int, data)
                break
            time.sleep(0.1)

        # cancel jobs
        self.sc.cancelAllJobs()
        t.join()

        for i in range(50):
            try:
                os.kill(worker_pid, 0)
                time.sleep(0.1)
            except OSError:
                break  # worker was killed
        else:
            self.fail("worker has not been killed after 5 seconds")

        try:
            os.kill(daemon_pid, 0)
        except OSError:
            self.fail("daemon had been killed")

    def test_fd_leak(self):
        N = 1100  # fd limit is 1024 by default
        rdd = self.sc.parallelize(range(N), N)
        self.assertEquals(N, rdd.count())


class TestSparkSubmit(unittest.TestCase):

    def setUp(self):
        self.programDir = tempfile.mkdtemp()
        self.sparkSubmit = os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")

    def tearDown(self):
        shutil.rmtree(self.programDir)

    def createTempFile(self, name, content):
        """
        Create a temp file with the given name and content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def createFileInZip(self, name, content):
        """
        Create a zip archive containing a file with the given content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name + ".zip")
        with zipfile.ZipFile(path, 'w') as zip:
            zip.writestr(name, content)
        return path

    def test_single_script(self):
        """Submit and test a single script file"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)

    def test_script_with_local_functions(self):
        """Submit and test a single script file calling a global function"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 3
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[3, 6, 9]", out)

    def test_module_dependency(self):
        """Submit and test a script with a dependency on another module"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen([self.sparkSubmit, "--py-files", zip, script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_module_dependency_on_cluster(self):
        """Submit and test a script with a dependency on another module on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen([self.sparkSubmit, "--py-files", zip, "--master",
                                "local-cluster[1,1,512]", script],
                                stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_single_script_on_cluster(self):
        """Submit and test a single script on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 2
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        proc = subprocess.Popen(
            [self.sparkSubmit, "--master", "local-cluster[1,1,512]", script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """General PySpark tests that depend on scipy """

    def test_serialize(self):
        from scipy.special import gammaln
        x = range(1, 5)
        expected = map(gammaln, x)
        observed = self.sc.parallelize(x).map(gammaln).collect()
        self.assertEqual(expected, observed)


@unittest.skipIf(not _have_numpy, "NumPy not installed")
class NumPyTests(PySparkTestCase):

    """General PySpark tests that depend on numpy """

    def test_statcounter_array(self):
        x = self.sc.parallelize([np.array([1.0, 1.0]), np.array([2.0, 2.0]), np.array([3.0, 3.0])])
        s = x.stats()
        self.assertSequenceEqual([2.0, 2.0], s.mean().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.min().tolist())
        self.assertSequenceEqual([3.0, 3.0], s.max().tolist())
        self.assertSequenceEqual([1.0, 1.0], s.sampleStdev().tolist())


if __name__ == "__main__":
    if not _have_scipy:
        print "NOTE: Skipping SciPy tests as it does not seem to be installed"
    if not _have_numpy:
        print "NOTE: Skipping NumPy tests as it does not seem to be installed"
    unittest.main()
    if not _have_scipy:
        print "NOTE: SciPy tests were skipped as it does not seem to be installed"
    if not _have_numpy:
        print "NOTE: NumPy tests were skipped as it does not seem to be installed"
