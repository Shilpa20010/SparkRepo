import unittest
from Spark_Assignment.Source.Assignment1.utils import *
from pyspark.sql.types import *


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestAssignment1").getOrCreate()

        #creating user table schema

        self.userschema = StructType([
            StructField("user_id",IntegerType(),True),
            StructField("emailid",StringType(),True),
            StructField("nativelanguage",StringType(),True),
            StructField("location",StringType(),True)])




        #user table data
        self.userdata = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")]

        #creating transaction table schema

        self.transactionschema = StructType([
            StructField("transaction_id",IntegerType(),True),
            StructField("product_id",IntegerType(),True),
            StructField("userid",IntegerType(),True),
            StructField("price",IntegerType(),True),
            StructField("product_description",StringType(),True)
        ])


        #creating transaction table data

        self.transactiondata = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")]


        #Total spending done by each user on each product

        self.totalschema = StructType([StructField("userid",IntegerType(),True),
                                       StructField("product_description",StringType(),True),
                                       StructField("sum(price)",IntegerType(),True)])

        self.totaldata = ([(101, "fridge", 35000),
                            (101, "mouse",700),
                            (102, "keyboard", 900),
                            (103, "tv", 34000),
                            (105, "sofa", 55000)])

        #product per user

        self.productschema = StructType([StructField("userid",IntegerType(),True),
                                         StructField("collect_list(product_description)",ArrayType(StringType()),True)])

        self.productdata = ([
                            (101, ['mouse', 'fridge']),
                            (103, ['tv']),
                            (102, ['keyboard']),
                            (105, ['sofa'])])

        # To get unique location

        self.uniqueschema = StructType([StructField("product_id",IntegerType(),True),StructField("count_by_column",IntegerType(),True)])

        self.uniquedata = ([(1000005, 1),
                            (1000003, 1),
                            (1000001, 1),
                            (1000002, 1),
                            (1000004, 1)])

        self.user_df = self.spark.createDataFrame(data= self.userdata,schema= self.userschema)
        self.transaction_df = self.spark.createDataFrame(data=self.transactiondata,schema=self.transactionschema)

        self.joined_df = self.transaction_df.join(self.user_df,self.transaction_df["userid"]==self.user_df["user_id"])

        self.total_df = self.spark.createDataFrame(data=self.totaldata,schema=self.totalschema)
        self.product_df = self.spark.createDataFrame(data=self.productdata,schema=self.productschema)
        self.unique_df = self.spark.createDataFrame(data=self.uniquedata,schema=self.uniqueschema)

    # def tearDownClass(self):
    #     self.spark.stop()


    #Test for product location

    def test_productlocation(self):
        actual_data = productlocation(self.joined_df,"product_id","location")
        self.assertEqual(sorted(self.unique_df.collect()),sorted(actual_data.collect()))


    #Test for user products

    def test_user_products(self):
        expected_result = prouduct_user(self.joined_df,"userid","product_description")
        self.assertEqual(sorted(self.product_df.collect()),sorted(expected_result.collect()))

    def test_user_spending(self):
        expected1 = spending_user(self.joined_df,"userid","product_description","price")
        self.assertEqual(sorted(self.total_df.collect()),sorted(expected1.collect()))



if __name__ == '__main__':
    unittest.main()
