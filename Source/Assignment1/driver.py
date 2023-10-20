from Spark_Assignment.Source.Assignment1.utils import *

#I am Initalizing the Session

spark = start_session()

#Path of csv files

path_user = r"C:\Users\ShilpaJoshi\PycharmProjects\pythonProject2\Spark_Assignment\Resource\user.csv"
path_transaction = r"C:\Users\ShilpaJoshi\PycharmProjects\pythonProject2\Spark_Assignment\Resource\transaction.csv"

#Read the file

users_df,transactions_df = readfile(spark,path_user,path_transaction)

#Join the user and transaction df

primary_key = "user_id"
forigen_key = "userid"
join_df = jointable(transactions_df,users_df,primary_key,forigen_key)

#Count the unique location where product is sold

group_by_column = "product_id"
count_by_column = "location "
yd = productlocation(join_df,group_by_column,count_by_column)
yd.show()
yd.printSchema()

# Find the count of product by each user

group_by_ = "userid"
agg = "product_description"
y = prouduct_user(join_df,group_by_,agg)
y.show()
y.printSchema()

# Total spending done by each user per product

group_by_1 = "userid"
group_by_2 = "product_description"
agg = "price"
x = spending_user(join_df,group_by_1,group_by_2,agg)
x.show()
x.printSchema()

# Stopping the session

stopsession(spark)

