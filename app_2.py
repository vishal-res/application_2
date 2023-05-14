#!/usr/bin/env python
# coding: utf-8

# # Description of Data Sets:

# >> Data set for links between Wikipedia articles (enwiki-2013.txt): This dataset contains information about the links between Wikipedia articles. Each line represents a directed edge in the graph, indicating a link from one article to another.

# >> Data set with names of the articles (enwiki-2013-names.csv): This dataset contains the names of the articles in the same order as the corresponding node IDs in the link dataset.

# # PySpark based code for identifying top 1000 most linked articles in Wikipedia:-



# Importing nessasary libraires ->-> note :- os librairy is optional

import os
import pyspark
from pyspark.sql import SparkSession



# Creating Spark-Session

spark = SparkSession.builder.appName('test').getOrCreate()


# importing spark functions

from pyspark.sql.functions import *



# reading text data set from system and spliting it by ' ', since it is a text file (unstractured data).

# data set name:- "enwiki-2013.txt"

link_records = spark.read.text("/home/vishalp/Videos/assinment/enwiki-2013.txt").selectExpr("split(value, ' ') as link")



#  Filter out comments and header

filtered_links = link_records.filter(~(col("value").startswith("#") | (col("value") == "FromNodeId")))



# Extract article links

articles_records = filtered_links.select(filtered_links.link[0].cast("int").alias("from_article"),
                                         filtered_links.link[1].cast("int").alias("to_article"))


# Read the article names dataset

# data set name:- enwiki-2013-names.csv

names_data = spark.read.csv("/home/vishalp/Videos/assinment/enwiki-2013-names.csv", header=True)


# rename the columns

names_data = names_data.withColumnRenamed('node_id', 'to_article').withColumnRenamed('name', 'article_name')


# Join link data with article names -->> joining two tables

linked_articles_with_names = linked_articles.join(names_data, linked_articles.from_article == names_data.article_id)     .select("to_article", "article_name")



# Count occurrences of each article

article_counts = linked_articles_with_names.groupBy("to_article", "article_name").count()



# Sort articles by count in descending order

sorted_articles = article_counts.sort("count", ascending=False)



# Take the top 1000 most linked articles

top_1000_articles = sorted_articles.limit(1000).collect()



# Display the top 1000 articles

for article in top_1000_articles:
    print(article.article_name, article["count"])



# Stop the SparkSession

spark.stop()

