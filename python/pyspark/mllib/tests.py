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
Fuller unit tests for Python MLlib.
"""

from numpy import array, array_equal
import unittest

from pyspark.mllib._common import _convert_vector, _serialize_double_vector, \
    _deserialize_double_vector, _dot, _squared_distance
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.tests import PySparkTestCase


_have_scipy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass


class VectorTests(unittest.TestCase):

    def test_serialize(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = array([1., 2., 3., 4.])
        lst = [1, 2, 3, 4]
        self.assertTrue(sv is _convert_vector(sv))
        self.assertTrue(dv is _convert_vector(dv))
        self.assertTrue(array_equal(dv, _convert_vector(lst)))
        self.assertEquals(sv, _deserialize_double_vector(_serialize_double_vector(sv)))
        self.assertTrue(array_equal(dv, _deserialize_double_vector(_serialize_double_vector(dv))))
        self.assertTrue(array_equal(dv, _deserialize_double_vector(_serialize_double_vector(lst))))

    def test_dot(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = array([1., 2., 3., 4.])
        lst = [1, 2, 3, 4]
        mat = array([[1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.]])
        self.assertEquals(10.0, _dot(sv, dv))
        self.assertTrue(array_equal(array([3., 6., 9., 12.]), _dot(sv, mat)))
        self.assertEquals(30.0, _dot(dv, dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), _dot(dv, mat)))
        self.assertEquals(30.0, _dot(lst, dv))
        self.assertTrue(array_equal(array([10., 20., 30., 40.]), _dot(lst, mat)))

    def test_squared_distance(self):
        sv = SparseVector(4, {1: 1, 3: 2})
        dv = array([1., 2., 3., 4.])
        lst = [4, 3, 2, 1]
        self.assertEquals(15.0, _squared_distance(sv, dv))
        self.assertEquals(25.0, _squared_distance(sv, lst))
        self.assertEquals(20.0, _squared_distance(dv, lst))
        self.assertEquals(15.0, _squared_distance(dv, sv))
        self.assertEquals(25.0, _squared_distance(lst, sv))
        self.assertEquals(20.0, _squared_distance(lst, dv))
        self.assertEquals(0.0, _squared_distance(sv, sv))
        self.assertEquals(0.0, _squared_distance(dv, dv))
        self.assertEquals(0.0, _squared_distance(lst, lst))


class ListTests(PySparkTestCase):

    """
    Test MLlib algorithms on plain lists, to make sure they're passed through
    as NumPy arrays.
    """

    def test_clustering(self):
        from pyspark.mllib.clustering import KMeans
        data = [
            [0, 1.1],
            [0, 1.2],
            [1.1, 0],
            [1.2, 0],
        ]
        clusters = KMeans.train(self.sc.parallelize(data), 2, initializationMode="k-means||")
        self.assertEquals(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEquals(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(0.0, [1, 0, 0]),
            LabeledPoint(1.0, [0, 1, 1]),
            LabeledPoint(0.0, [2, 0, 0]),
            LabeledPoint(1.0, [0, 2, 1])
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        lr_model = LogisticRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        svm_model = SVMWithSGD.train(rdd)
        self.assertTrue(svm_model.predict(features[0]) <= 0)
        self.assertTrue(svm_model.predict(features[1]) > 0)
        self.assertTrue(svm_model.predict(features[2]) <= 0)
        self.assertTrue(svm_model.predict(features[3]) > 0)

        nb_model = NaiveBayes.train(rdd)
        self.assertTrue(nb_model.predict(features[0]) <= 0)
        self.assertTrue(nb_model.predict(features[1]) > 0)
        self.assertTrue(nb_model.predict(features[2]) <= 0)
        self.assertTrue(nb_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 3}  # feature 0 has 3 categories
        dt_model = \
            DecisionTree.trainClassifier(rdd, numClasses=2,
                                         categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

    def test_regression(self):
        from pyspark.mllib.regression import LinearRegressionWithSGD, LassoWithSGD, \
            RidgeRegressionWithSGD
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(-1.0, [0, -1]),
            LabeledPoint(1.0, [0, 1]),
            LabeledPoint(-1.0, [0, -2]),
            LabeledPoint(1.0, [0, 2])
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        lr_model = LinearRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        lasso_model = LassoWithSGD.train(rdd)
        self.assertTrue(lasso_model.predict(features[0]) <= 0)
        self.assertTrue(lasso_model.predict(features[1]) > 0)
        self.assertTrue(lasso_model.predict(features[2]) <= 0)
        self.assertTrue(lasso_model.predict(features[3]) > 0)

        rr_model = RidgeRegressionWithSGD.train(rdd)
        self.assertTrue(rr_model.predict(features[0]) <= 0)
        self.assertTrue(rr_model.predict(features[1]) > 0)
        self.assertTrue(rr_model.predict(features[2]) <= 0)
        self.assertTrue(rr_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 2}  # feature 0 has 2 categories
        dt_model = \
            DecisionTree.trainRegressor(rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):

    """
    Test both vector operations and MLlib algorithms with SciPy sparse matrices,
    if SciPy is available.
    """

    def test_serialize(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        sv = SparseVector(4, {1: 1, 3: 2})
        self.assertEquals(sv, _convert_vector(lil))
        self.assertEquals(sv, _convert_vector(lil.tocsc()))
        self.assertEquals(sv, _convert_vector(lil.tocoo()))
        self.assertEquals(sv, _convert_vector(lil.tocsr()))
        self.assertEquals(sv, _convert_vector(lil.todok()))
        self.assertEquals(sv, _deserialize_double_vector(_serialize_double_vector(lil)))
        self.assertEquals(sv, _deserialize_double_vector(_serialize_double_vector(lil.tocsc())))
        self.assertEquals(sv, _deserialize_double_vector(_serialize_double_vector(lil.tocsr())))
        self.assertEquals(sv, _deserialize_double_vector(_serialize_double_vector(lil.todok())))

    def test_dot(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 1
        lil[3, 0] = 2
        dv = array([1., 2., 3., 4.])
        sv = SparseVector(4, {0: 1, 1: 2, 2: 3, 3: 4})
        mat = array([[1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.],
                     [1., 2., 3., 4.]])
        self.assertEquals(10.0, _dot(lil, dv))
        self.assertTrue(array_equal(array([3., 6., 9., 12.]), _dot(lil, mat)))

    def test_squared_distance(self):
        from scipy.sparse import lil_matrix
        lil = lil_matrix((4, 1))
        lil[1, 0] = 3
        lil[3, 0] = 2
        dv = array([1., 2., 3., 4.])
        sv = SparseVector(4, {0: 1, 1: 2, 2: 3, 3: 4})
        self.assertEquals(15.0, _squared_distance(lil, dv))
        self.assertEquals(15.0, _squared_distance(lil, sv))
        self.assertEquals(15.0, _squared_distance(dv, lil))
        self.assertEquals(15.0, _squared_distance(sv, lil))

    def scipy_matrix(self, size, values):
        """Create a column SciPy matrix from a dictionary of values"""
        from scipy.sparse import lil_matrix
        lil = lil_matrix((size, 1))
        for key, value in values.items():
            lil[key, 0] = value
        return lil

    def test_clustering(self):
        from pyspark.mllib.clustering import KMeans
        data = [
            self.scipy_matrix(3, {1: 1.0}),
            self.scipy_matrix(3, {1: 1.1}),
            self.scipy_matrix(3, {2: 1.0}),
            self.scipy_matrix(3, {2: 1.1})
        ]
        clusters = KMeans.train(self.sc.parallelize(data), 2, initializationMode="k-means||")
        self.assertEquals(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEquals(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(0.0, self.scipy_matrix(2, {0: 2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0}))
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features for p in data]

        lr_model = LogisticRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        svm_model = SVMWithSGD.train(rdd)
        self.assertTrue(svm_model.predict(features[0]) <= 0)
        self.assertTrue(svm_model.predict(features[1]) > 0)
        self.assertTrue(svm_model.predict(features[2]) <= 0)
        self.assertTrue(svm_model.predict(features[3]) > 0)

        nb_model = NaiveBayes.train(rdd)
        self.assertTrue(nb_model.predict(features[0]) <= 0)
        self.assertTrue(nb_model.predict(features[1]) > 0)
        self.assertTrue(nb_model.predict(features[2]) <= 0)
        self.assertTrue(nb_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 3}  # feature 0 has 3 categories
        dt_model = DecisionTree.trainClassifier(rdd, numClasses=2,
                                                categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

    def test_regression(self):
        from pyspark.mllib.regression import LinearRegressionWithSGD, LassoWithSGD, \
            RidgeRegressionWithSGD
        from pyspark.mllib.tree import DecisionTree
        data = [
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -1.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 1.0})),
            LabeledPoint(-1.0, self.scipy_matrix(2, {1: -2.0})),
            LabeledPoint(1.0, self.scipy_matrix(2, {1: 2.0}))
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features for p in data]

        lr_model = LinearRegressionWithSGD.train(rdd)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        lasso_model = LassoWithSGD.train(rdd)
        self.assertTrue(lasso_model.predict(features[0]) <= 0)
        self.assertTrue(lasso_model.predict(features[1]) > 0)
        self.assertTrue(lasso_model.predict(features[2]) <= 0)
        self.assertTrue(lasso_model.predict(features[3]) > 0)

        rr_model = RidgeRegressionWithSGD.train(rdd)
        self.assertTrue(rr_model.predict(features[0]) <= 0)
        self.assertTrue(rr_model.predict(features[1]) > 0)
        self.assertTrue(rr_model.predict(features[2]) <= 0)
        self.assertTrue(rr_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 2}  # feature 0 has 2 categories
        dt_model = DecisionTree.trainRegressor(rdd, categoricalFeaturesInfo=categoricalFeaturesInfo)
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)


if __name__ == "__main__":
    if not _have_scipy:
        print "NOTE: Skipping SciPy tests as it does not seem to be installed"
    unittest.main()
    if not _have_scipy:
        print "NOTE: SciPy tests were skipped as it does not seem to be installed"
