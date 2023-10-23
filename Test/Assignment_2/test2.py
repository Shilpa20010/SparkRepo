import unittest

from Spark_Assignment.Source.Assignment2.utils import *
from pyspark.sql import SparkSession


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = start_session()
        self.rdd_1 = read_rdd_file(self.log_path,self.spark)
        count_lines = 281234

        #Testing count RDD lines

        actua_input_count = count_lines_rdd(self.rdd_1)
        expect_output = count_lines
        self.assertEqual(actua_input_count,expect_output)

    def tearDown(self):
        self.spark.stop()


    def test_count_rdd(self):
        expected_count = 281234
        actual_Count = count_lines_rdd(self.rdd_1)
        self.assertEqual(actual_Count,expected_count)


    def test_warning(self):
        expected_count = 3811
        actual_count = count_warning(self.rdd_1)
        self.assertEqual(actual_count,expected_count)

    def test_numrepo(self):
        expected_count = 37596
        actual_processed_repo = count_repo(self.rdd_1)
        self.assertEqual(actual_processed_repo,expected_count)

    def test_most_active(self):
        expected_result = "api_client.rb: Unauthorised request with token: 46f11b5791b7db9077f4d9a9ab27f93e89dccad4"
        actual_client = most_HTTP(self.rdd_1)
        self.assertEqual(actual_client,expected_result)

    def test_most_failed(self):
        expected_result = "api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1749"
        actual_client = fail_HTTP(self.rdd_1)
        self.assertEqual(actual_client,expected_result)

    def test_most_active_hour(self):
        expected_result = "10"
        actual_result = most_Active_hour(self.rdd_1)
        self.assertEqual(actual_result,expected_result)

    def test_active_repo(self):
        expected_result = "130023"
        actual_result = most_active_repo(self.rdd_1)
        self.assertEqual(actual_result,expected_result)

    #
    # def test_something(self):
    #     self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
