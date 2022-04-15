#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 13:41:36 2022

@author: sijingyu
"""

from pyspark.sql.session import SparkSession
from pyspark import SparkContext,SparkConf

def moviecount():
    sc=SparkContext()
    spark=SparkSession.builder.appName("movie").getOrCreate()
    df=spark.read.csv('dataset/allData.csv',header=True)
    df=df.filter(df['userID']==4439)
    df_list=df.rdd.map(lambda x:(x.genre1,1)).reduceByKey(lambda x,y:x+y)
    #df_list=df.rdd.map(lambda x:((x.userID,x.genre1),1)).reduceByKey(lambda x,y:x+y)
    # df_list.collect()
    # #df2=df.groupBy('userID','genre1').count()
    # df_list.saveAsTextFile("one_person_list5.txt")
    moviecount_dict = df_list.collectAsMap()
    sc.stop()
    return moviecount_dict


