# Databricks notebook source
#Q2

from pyspark.sql import SparkSession
import statistics
spark = SparkSession.builder.appName("LowestCommonFriends").getOrCreate()
file_location = "/FileStore/tables/mutual.txt"
#reading the mutual file
input_data = spark.read.text(file_location)
#splitting the data by ","
user_friend_pairs = input_data.rdd.map(lambda x: tuple(x.value.split('\t'))).map(lambda x: (int(x[0]), x[1].split(',')))
#function that creates pairs
def creating_pair(pair):
    pairs = []
    for f in pair[1]:
        if f:
          friend_id = int(f)
          if pair[0] < friend_id:
            pairs.append(["{0},{1}".format(pair[0], friend_id),pair[1]])
          else:
            pairs.append(["{0},{1}".format(friend_id, pair[0]),pair[1]])
    return pairs

#function that return user and thier common friends
def common_friends(friends_list):
    user, friends = friends_list
    mutual_friends = set(friends[0]).intersection(*friends[1:])
    return user, len(mutual_friends)

#creating user pair and grouping it
user_created_freinds_pairs= user_friend_pairs.flatMap(creating_pair).groupByKey()
#computing user and thier common friends
common_friendsList=user_created_freinds_pairs.mapValues(list).map(common_friends)
#collecting the number of common friends
lengths_List=common_friendsList.map(lambda x: x[1]).collect()
#sorting the number of common friends list
lengths_List.sort()
#computing the median
median = statistics.median(lengths_List)
#computing the users that are having common friends less than median
result = common_friendsList.filter(lambda x: x[1]< median).map(lambda x: "\t".join(map(str,x)))
#saving the result to single partition file
result.coalesce(1).saveAsTextFile("/FileStore/tables/Output_2")
