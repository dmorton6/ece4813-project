from pyspark import SparkContext
from operator import add
from pyspark.mllib.clustering import KMeans
import numpy as np
from numpy import array
from math import sqrt
import sys
import pickle
import re
import datetime
from pymongo import MongoClient
sc = SparkContext("local", "Simple App")
client = MongoClient('mongodb://52.5.145.180:27017/')
print client.test_database
db = client.main_db
songs = db.songs_v2
#post_id=songs.insert_one({"test":"hello world"}).inserted_id
#print post_id

def g(x):
     print x

def line_strip(p):
    a = p[0].encode('ascii')
    b = p[1].encode('ascii')
    c = (p[2].encode('ascii')).strip(" ")
    d = p[3].encode('ascii')
    e=float(p[4])
    f=float(p[5])
    g=float(p[6])
    h=float(p[7])
    i=int(p[8].encode('ascii'))
    k = p[9].encode('ascii')
    j=(a,b,c,d,e,f,g,h,i,k)
    return j

def euclid_dist(p,user_point,center):
    #print p
    #print user_point
    #print center
    a=user_point-center
    b=p-center
    c=np.array([(user_point[0]/center[0]-1),(user_point[1]/center[1]-1),(user_point[2]/center[2]-1),(user_point[3]/center[3]-1)])
    d=np.array([(p[0]/center[0]-1),(p[1]/center[1]-1),(p[2]/center[2]-1),(p[3]/center[3]-1)])
    dist1=np.linalg.norm(c)
    dist2= np.linalg.norm(d)
    dot=np.dot(c,d)
    cosine=dot/dist1/dist2
    similarity=np.sqrt(cosine*cosine+(dist1-dist2)*(dist1-dist2))
    return str(similarity)

#PD4_read=sc.textFile("file:///home/ubuntu/music_data_sequence/part-00002")
PD4_read=sc.textFile("file:////home/ubuntu/spark-1.2.0.2.2.0.0-82-bin-2.6.0.2.2.0.0-2041/save_v3.csv")
PD4_read_count=PD4_read.count()
PD4_iter=PD4_read.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line:line_strip(line)).collect()
#PD4_count=PD4.count()
#print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',PD1_count,' ',PD2_count,' ',PD3_count,' ',PD4_count,' ',PD4_read_count,' ','*******************@@@@@@@@@@@@@@@@@@@@@@@'
#print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',PD4_read.count(),'*******************@@@@@@@@@@@@@@@@@@@@@@@'
#PD4_read.foreach(g)
#input_user=sys.argv[1]
for item in range(len(PD4_iter)):
	#print PD4_iter[item][2]
	input_user=PD4_iter[item][2]
#print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',input_user ,'*******************@@@@@@@@@@@@@@@@@@@@@@@'
#PD4_filt=PD4.filter(lambda line: line[0]==sys.argv[1])
#PD4_filt2=PD4.map(lambda line:line[0])
	PD4_read2=PD4_read.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line:line_strip(line))
#PD4_read2.foreach(g)

#PD4_read1=PD4_read2.map(lambda line: (((line[0].encode('utf-8')).replace(' ','').replace('\'','')),(str(line[1]).strip(' ').strip('\'')),float(line[2]),float(line[3]),line[4]))
#PD4_read1=PD4_read2.map(lambda line:(((line[0].encode('utf-8')).strip(' ').strip('\'').strip('(\'')),(line[0].encode('utf-8')).strip(' ').strip('\'').strip('(\'')))
#PD4_read1=PD4_read2.map(lambda line:line[1])
#PD4_read1.foreach(g)
#PD4_read2.map(lambda line:(str(line[1]).strip(' ').strip('\''))).foreach(g)
	PD4_filt=PD4_read2.filter(lambda line:(line[2]==input_user))
	PD4_filt.foreach(g)

#PD4_filt.foreach(g)
#CF=PD4_filt.foreach(g)
	CF=PD4_filt.collect()
#print'Type of Cluster Found is ',type(CF)
	Cl_found=CF[0][8]
	print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%% Cluster Found=',Cl_found,'*******************@@@@@@@@@@@@@@@@@@@@@@@'
	cc = pickle.load(open("/home/ubuntu/cluster_centers.p", "rb" ))
	print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%% Cluster Center=',type(cc),'*******************@@@@@@@@@@@@@@@@@@@@@@@', 
	print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%% Cluster Center=',cc[Cl_found][0],',',cc[Cl_found][1],',',cc[Cl_found][2],',',cc[Cl_found][3],'',type(Cl_found),'*******************@@@@@@@@@@@@@@@@@@@@@@@'

	PD6=PD4_read2.filter(lambda line:(line[8]==Cl_found)).filter(lambda line:(line[2]!=input_user))
	#PD6.foreach(g)
	#print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',PD6.count(),'*******************@@@@@@@@@@@@@@@@@@@@@@@'

	#PD6.foreach(g)
	PD7=PD6.map(lambda p:(euclid_dist(np.asarray((p[4],p[5],p[6],p[7])),np.asarray((CF[0][4],CF[0][5],CF[0][6],CF[0][7])),cc[Cl_found]),(p[0],p[1],p[2],p[3]))).sortByKey().collect()
	#PD7.foreach(g)
#PD7=PD6.map(lambda p:(euclid_dist(p,CF[0],cc[Cl_found])))
#PD7.foreach(g)
#PD8=np.asarray(PD7)
#PD9=np.sort(PD8)
#print PD8
#print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',len(PD9),'*******************@@@@@@@@@@@@@@@@@@@@@@@'
#print 'The user input is ',CF[0][3],' by ',CF[0][1] 
	post = {"name": (CF[0][3]).encode('ascii'),
     		"artist":(CF[0][1]).encode('ascii'),
     		"spark-id":(CF[0][2]).encode('ascii'),
     		"year":(CF[0][9]).encode('ascii'),
     		"tags": ["mongodb", "python", "pymongo"],
     		"date": datetime.datetime.utcnow()}
	post["similar"]=[]
	print post
	print 'Similar Songs are ' 
	for item in range(10):
    	#print len(PD7[item])
    	#print ((PD7[item][1][3]).encode('ascii')).encode('ascii'),' by ',(PD7[item][1][1]).encode('ascii')
    		song = {"name": (PD7[item][1][3]).encode('ascii'),
        		"artist":(PD7[item][1][1]).encode('ascii'),
	        	"spark-id":(PD7[item][1][2]).encode('ascii')}
	    	post["similar"].append(song)

	print post
	post_id=songs.insert_one(post).inserted_id
	print post_id
'''
#PD1_1=PD1.filter(lambda line: len(line)>1)
#PD1_1.foreach(g)
#PD1_1.saveAsObjectFile("file:///home/ubuntu/music_data2")
#Parseddata.
#PD4_filt3.foreach(g)
#.reduceByKey(lambda a,b:a+b)
#PD1.foreach(g)
#File=sc.textFile("file:///home/ubuntu/music_data.csv").map(myFunc)
#File.foreach(g)
#logFile = "file:///home/ubuntu/music_data.csv"  # Should be some file on your system
'''
logData = sc.textFile("file:///home/ubuntu/music_data.csv").cache()
#numAs = logData.filter(lambda s: 'a' in s).count()
#numBs = logData.filter(lambda s: 'b' in s).count()
#print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
sc.stop()
