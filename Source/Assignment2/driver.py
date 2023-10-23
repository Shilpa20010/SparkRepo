from Spark_Assignment.Source.Assignment2.utils import *

sc = start_session()
log_path = r"C:\Users\ShilpaJoshi\PycharmProjects\pythonProject2\Spark_Assignment\Resource\ghtorrent-logs.txt"

#Reading the rdd file

rdd_1 = read_rdd_file(log_path,sc)

#Counting the number of lines in RDD file

log_file2 = count_lines_rdd(rdd_1)
print(log_file2)

#Counting the number of warning in RDD file

log_file3 = count_warning(rdd_1)
print(log_file3)

#Counting the number of repo in file

log_file4 = count_repo(rdd_1)
print(log_file4) # This is output of ("api_client") not ("api_client.rb")

#Counting most active HTTP

log_file5 = most_HTTP(rdd_1)
print(log_file5)

#Counting most failed requests

log_file_6 = fail_HTTP(rdd_1)
print(log_file_6)

#Hourly Count request

log_file_7 = most_Active_hour(rdd_1)
print(log_file_7)

#Most Active repo

log_file_8 = most_active_repo(rdd_1)
print(log_file_8)

#stoping session

sc.stop()