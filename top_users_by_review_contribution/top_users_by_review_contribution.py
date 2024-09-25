# Databricks notebook source
#Q5
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Top 10 Contirbuters").getOrCreate()
reviews_file_location = "/FileStore/tables/review.csv"
users_file_location = "/FileStore/tables/user.csv"
#reading of two files
reviews_data = spark.read.text(reviews_file_location)
users_data = spark.read.text(users_file_location)
#splitting the review dataset by "::" and creating userid as key and 1 as values and performing reduce operation on userids
reviews_users=reviews_data.rdd.map(lambda x : x.value.split("::")).map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y)
#splitting the user dataset by "::" 
users = users_data.rdd.map(lambda x : x.value.split("::"))
#computing total reviews
total_reviews= sum(reviews_users.map(lambda x: x[1]).collect())
#computing total contribution per user
contributers=reviews_users.map(lambda x: (x[0],(x[1]/total_reviews)*100))
#sorting and getting the top 10 contributers
top_10_names_contributers=users.join(contributers).sortBy(lambda x : x[1][1],ascending=False).map(lambda x:"{}".format(x[1][0]).ljust(20)+"{}".format(x[1][1])).take(10)
#adding header
top_10_names_contributers.insert(0,"Name".ljust(20)+"Contribution")
#converting list to spark rdd
top_ten_contributers = spark.sparkContext.parallelize(top_10_names_contributers)
#saving the result to single partition file
top_ten_contributers.coalesce(1).map(lambda r: str(r)).saveAsTextFile("/FileStore/tables/Output_5")
