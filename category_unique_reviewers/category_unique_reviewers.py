# Databricks notebook source
#Q6
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Lowest Ratings for Category based on unique number of Users").getOrCreate()
business_file_location = "/FileStore/tables/business.csv"
reviews_file_location = "/FileStore/tables/review.csv"
business_data = spark.read.text(business_file_location)
reviews_data = spark.read.text(reviews_file_location)
#splitting the business by "::"" dataset and creating business id as key and category as value
business_ids=business_data.rdd.map(lambda x : x.value.split("::")).flatMap(lambda x: [(x[0], i.strip()) for i in x[2][5:-1].split(",")])
#splitting the reviews dataset by "::" and creating business id as key and user id and rating as values
reviews=reviews_data.rdd.map(lambda x : x.value.split("::")).map(lambda x : (x[2],(x[1],float(x[3]))))
#joining reviews and business rdds and grouping by category.
grouped_ratings_user= reviews.join(business_ids).map(lambda x: (x[1][1],(x[1][0][0],x[1][0][1]))).groupByKey().mapValues(lambda x: list(set(x)))
#computing the min rating among all the ratings
resutls_categories=grouped_ratings_user.map(lambda x: "{}".format(x[0]).ljust(40)+"{}".format(len(x[1])).ljust(35)+"{}".format(min([i[1] for i in x[1]]))).collect()
#adding header
resutls_categories.insert(0,"Category".ljust(40)+"No.of_Unique_Reviewers".ljust(35)+"Lowest_Rating")
#converting list to spark rdd
results = spark.sparkContext.parallelize(resutls_categories)
#saving the result to single partition file
results.coalesce(1).saveAsTextFile("/FileStore/tables/Output_6")
