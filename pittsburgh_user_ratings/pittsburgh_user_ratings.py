# Databricks notebook source
#Q3
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Average Ratings Pittsburgh").getOrCreate()
business_file_location = "/FileStore/tables/business.csv"
reviews_file_location = "/FileStore/tables/review.csv"
users_file_location = "/FileStore/tables/user.csv"
#reading of three files
business_data = spark.read.text(business_file_location)
reviews_data = spark.read.text(reviews_file_location)
users_data = spark.read.text(users_file_location)
#splitting the business dataset by "::" and filtering the rows that have address Pittsburgh and creating business id as key and adress as values
business_id_pittsburgh=business_data.rdd.map(lambda x : x.value.split("::")).filter(lambda x: "Pittsburgh" in x[1]).map(lambda x: (x[0],x[1]))
#splitting the review dataset by "::" and creating business id as key and user and ratings are values
reviews=reviews_data.rdd.map(lambda x : x.value.split("::")).map(lambda x : (x[2],(x[1],float(x[3]))))
#splitting the user dataset by "::"
users = users_data.rdd.map(lambda x : x.value.split("::"))
#joining reviews and business dataset by business id and grouping by userids.
grouped_ratings_user= reviews.join(business_id_pittsburgh).map(lambda x : (x[1][0][0],x[1][0][1])).groupByKey()
#comptung average ratings for particular user ids.
averaged_ratings= grouped_ratings_user.mapValues(list).mapValues(lambda x : (sum(x)/len(x)))
#joining users and average ratings
results=users.join(averaged_ratings).map(lambda x: "{}".format(x[1][0]).ljust(20)+"{}".format(x[1][1])).collect()
#adding header
results.insert(0,"Username".ljust(20)+"Rating")
#converting list to spark rdd
results_rdd = spark.sparkContext.parallelize(results)
#saving the result to single partition file
results_rdd.coalesce(1).saveAsTextFile("/FileStore/tables/Output_3")
