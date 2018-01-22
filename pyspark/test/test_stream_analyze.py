import unittest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
import pyspark.sql.functions as fn
from stream_analyze import *
from utils import *

class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[2]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = SparkSession.builder.appName("PythonZR").getOrCreate()
        test_input = readFileToList('../data/file2.txt')
        input_rdd = cls.sc.parallelize(test_input, 1)\
        .map(lambda line:line.split("\t"))\
        .map(lambda (url, user_id, timestamp, type):(url, user_id, eval(timestamp), type))
        l = input_rdd.collect()
        print l
        cls.df = cls.spark.createDataFrame(input_rdd).toDF('url', 'user_id', 'timestamp', 'type')
        windowval = Window.partitionBy('user_id').orderBy('timestamp').rangeBetween(Window.unboundedPreceding, 0)
        cls.dfSessionized = cls.df.withColumn('session_id', fn.sum(fn.when(cls.df["type"] == 'start_session', 1).otherwise(0)).over(windowval))\
        .groupBy('user_id','session_id')\
        .agg(fn.collect_list(fn.struct('type', 'url','timestamp')).alias('path'))


    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

class SimpleTestCase(PySparkTestCase):

    def test_extractTopConvertingUsers(self):
        topConverting =  [x.asDict() for x in extractTopConvertingUsers(self.df,3).collect()]
        expected = [{'count': 3, 'user_id': u'alan'},{'count': 2, 'user_id': u'alex'},{'count': 1, 'user_id': u'ran'}]
        self.assertEqual(topConverting, expected)
        return
    
    def test_minConversion(self):
        pathConv1 = [{'type':'start_session', 'url':'ziprecruiter.com'},{'type':'conversion', 'url':'ziprecruiter.com'}]
        self.assertEqual(minConversion(pathConv1), 1)
        pathConv0 = [{'type':'conversion', 'url':'ziprecruiter.com'},{'type':'conversion', 'url':'ziprecruiter.com'}]
        self.assertEqual(minConversion(pathConv0), 0)
        pathConvNone = [{'type':'start_session', 'url':'ziprecruiter.com'},{'type':'in_page', 'url':'ziprecruiter.com'}]
        self.assertEqual(minConversion(pathConvNone), -1)
        
    def test_extractMinConversion(self):
        convertionDistancePerUser = [x.asDict() for x in extractMinConversion(self.dfSessionized).collect()]
        expected = [{'user_id':u'ran', 'conversion_distance':u'7'}, {'user_id':u'alan', 'conversion_distance':u'1'}, {'user_id':u'alex', 'conversion_distance':u'0'}]
        self.assertEqual(convertionDistancePerUser, expected)
        
if __name__ == '__main__':
    unittest.main()