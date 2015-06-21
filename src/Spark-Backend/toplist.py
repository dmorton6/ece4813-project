from pyspark import SparkContext
from operator import add
from pyspark.mllib.clustering import KMeans
import numpy as np
from numpy import array
from math import sqrt
import sys
import math
import pickle
import re
import datetime
from pymongo import MongoClient

sc = SparkContext("local", "Simple App")

client = MongoClient('mongodb://52.5.145.180:27017/')
print client.test_database
db = client.main_db
top_s = db.charts  #changed it from songs to top_songs

#sequence : artist_name,artist_id,song_name,song_id,artist_hottness,song_hottness,loudness,tempo,key,year
step1=sc.textFile("file:///home/ubuntu/output_csv.csv")
step2=step1.map(lambda line: line.split(",")).filter(lambda line: len(line)>1)
step3=step2.map(lambda line: ((str(line[1].encode('utf-8')),str(line[0].encode('utf-8')))))

step4=step2.map(lambda line: ((line[1].encode('utf-8'),line[0].encode('utf-8'),float(line[4]),int(line[9])))).distinct()
#step4.foreach(g)
#inp_year=int(sys.argv[1])

for i in range (10)
    inp_year = 1990 + i

    step5=step4.filter(lambda line: line[3]==inp_year).map(lambda line: ((float(line[2]),(line[0],line[1],line[3]))))
    step6=step5.sortByKey(False)
    step7=step6.take(10)
    step8=step2.map(lambda line: ((line[3].encode('utf-8'),line[2].encode('utf-8'),float(line[5]),int(line[9])))).distinct()
    step9=step8.filter(lambda line: line[3]==inp_year).map(lambda line: ((float(line[2]),(line[0],line[1],line[3])))).filter(lambda line: not math.isnan(line[0]))
    step10=step9.sortByKey(False)
    step11=step10.take(10)

    post = {"year": sys.argv[1].encode('ascii'),
     "tags": ["mongodb", "python", "pymongo"],
     "date": datetime.datetime.utcnow()}
    post["top-songs"]=[]
    post["top-artist"]=[]
    for i in step7:
        artist = {"Name": (i[1][1]).encode('ascii')}
        post["top-artist"].append(artist)

    for i in step11:
        song = {"Name": (i[1][1]).encode('ascii')}
        post["top-songs"].append(song)
    print post
    post_id=top_s.insert_one(post).inserted_id
sc.stop
