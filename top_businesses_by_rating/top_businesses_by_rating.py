# Databricks notebook source
#Q4
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Top 10 businesses average ratings.").getOrCreate()
business_file_location = "/FileStore/tables/business.csv"
reviews_file_location = "/FileStore/tables/review.csv"
#reading of two files
business_data = spark.read.text(business_file_location)
reviews_data = spark.read.text(reviews_file_location)
#splitting the business dataset by "::" and creating business id as key and categories as values
business_ids_catogeries=business_data.rdd.map(lambda x : x.value.split("::")).map(lambda x: (x[0],x[2][5:-1].split(", ")))
#splitting the review dataset by "::" and creating business id as key and ratings as values
reviews=reviews_data.rdd.map(lambda x : x.value.split("::")).map(lambda x : (x[2],(float(x[3]))))
#joining reviews and business dataset by business id and grouping by business ids
grouped_business_reviews= business_ids_catogeries.join(reviews).groupByKey()
#computing the average ratings acquired for particular business id
averaged_ratings=grouped_business_reviews.mapValues(list).mapValues(lambda l: (l[0][0],sum([i[1] for i in l])/len(l)))
#sorting and collecting top ten ratings
top_ten_results= averaged_ratings.sortBy(lambda x: x[1][1],ascending=False).map(lambda x:"{}".format(x[0]).ljust(30)+"List{}".format(x[1][0]).ljust(70)+"{}".format(x[1][1])).collect()[0:10]
#adding header
top_ten_results.insert(0,"Business_ID".ljust(30)+"Categories".ljust(70)+"AVG_Rating")
#converting list to spark rdd
top_ten_rdd = spark.sparkContext.parallelize(top_ten_results)
#saving the result to single partition file
top_ten_rdd.coalesce(1).map(lambda r: str(r)).saveAsTextFile("/FileStore/tables/Output_4")
