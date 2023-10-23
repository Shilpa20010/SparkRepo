from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import *
import logging


logging.basicConfig(filename=r"C:\Users\ShilpaJoshi\PycharmProjects\pythonProject2\Spark_Assignment\logs\spark.log",filemode="w",level=logging.INFO)
log = logging.getLogger()
log.info("This is an info message.")

def start_session():
    log.info("Spark Session Started")
    return SparkContext(conf=SparkConf().setAppName("Assignment_2"))

def read_rdd_file(log_path,spark):
    log.info("Reading file started")
    rdd_1 = spark.textFile(log_path)
    return rdd_1

def count_lines_rdd(rdd1):
    log.info("Counting lines for RDD started")
    total_lines_RDD = rdd1.count()
    return total_lines_RDD

def count_warning(rdd1):
    log.info("Counting the number of warning")
    total_Warning = rdd1.filter(lambda  line : "WARN" in line).count()
    return total_Warning

def count_repo(rdd1):
    log.info("Counting the repo is started")
    total_repo = rdd1.filter(lambda  line : "api_client" in line).count() #Here we can also write api_client.rb
    return total_repo

def most_HTTP(rdd1):
    log.info("Most active client searching started")
    most_active = rdd1.filter(lambda  line : "api_client" in line).map(lambda line : (line.split("--")[1].strip(),1))
    most_active_client = most_active.reduceByKey(lambda a,b : a+b).max(key = lambda x : x[1])
    return most_active_client[0]

def fail_HTTP(rdd1):
    log.info("Most Failed requests for HTTP started")
    most_fail = rdd1.filter(lambda line : "Failed request" in line)
    most_fail_client = most_fail.map(lambda line : (line.split("--")[1].strip(),1))
    most_fail_client_1 = most_fail_client.reduceByKey(lambda a,b : a+b).max(key = lambda x:x[1])
    return most_fail_client_1[0]

def most_Active_hour(rdd1):
    log.info("Most active per hour a day")
    valid_line = rdd1.filter(lambda line : len(line.split(","))>=2)
    count_hour = valid_line.map(lambda line : (line.split(",")[1].split("T")[1][:2],1))
    hour_count = count_hour.reduceByKey(lambda a,b : a+b).max(key = lambda x:x[1])
    return hour_count[0]

def most_active_repo(rdd1):
    log.info("Most active repo")
    repo_active = rdd1.filter(lambda line : 'ghtorrent.rb' in line and 'exists' in line).count()
    return repo_active


def stop_session(spark):
    log.info("Session is about to stop")
    return spark.stop()