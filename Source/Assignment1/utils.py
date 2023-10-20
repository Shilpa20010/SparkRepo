from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging

logging.basicConfig(filename=r"C:\Users\ShilpaJoshi\PycharmProjects\pythonProject2\Spark_Assignment\logs\spark.log",filemode="a",level=logging.INFO)
log = logging.getLogger()
log.info("This is an info message.")

#Function to start session
def start_session():
    log.info("Spark session started")
    return SparkSession.builder.appName("SparkAssignment").getOrCreate()

#Function to read files
def readfile(spark,user_path,transaction_path):
    users_df = spark.read.csv(user_path,header=True,inferSchema=True)
    transactions_df = spark.read.csv(transaction_path,header=True,inferSchema=True)
    log.info("Read the files")
    transactions_df.show()
    transactions_df.printSchema()
    users_df.show()
    users_df.printSchema()
    return transactions_df,users_df

#Function to joining two df
def jointable(transactions_df,users_df,primary_key,forigen_key):
    #Joined transactions df and users df
    joined_df = transactions_df.join(users_df,transactions_df[primary_key]==users_df[forigen_key])
    log.info("Joined two dataframes using userid")
    joined_df.show()
    joined_df.printSchema()
    return joined_df


#Function to Count of unique locations where each product is sold.

def productlocation(joined_df,group_by_column,count_by_column):
    group_by_column = group_by_column.strip()
    product_df = joined_df.groupBy(group_by_column).agg(countDistinct(count_by_column))
    product_df.printSchema()
    log.info("count the unique location where  each product is sold")
    return product_df

#Function to find out product bought by each user

def prouduct_user(joined_df,group_by_,agg):
    user_product = joined_df.groupBy(group_by_).agg({agg: "collect_list"})
    log.info("count the product bought by each user")
    user_product.printSchema()
    return user_product


#Function for total spending done by each user

def spending_user(joined_df,group_by_1,group_by_2,agg):
    spending_df = joined_df.groupBy(group_by_1,group_by_2).agg({agg : "sum"})
    log.info("Total spending done by user is calculated")
    # spending_df.printSchema()
    return spending_df

# Stopping session

def stopsession(spark):
    log.info("Spark session stopped")
    return spark.stop()









