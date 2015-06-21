from pyspark import SparkContext
from operator import add
from pyspark.mllib.clustering import KMeans
import numpy as np
from numpy import array
from math import sqrt
import sys
import pickle

sc = SparkContext("local", "Simple App")
def g(x):
     print x

def closestPoint(p,centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        #print p
        #print centers[i]
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex

Parseddata=sc.textFile("file:///home/ubuntu/music_data_updated_v2.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (float(line[4]),float(line[5]),float(line[6]),float(line[7])))
clusters = KMeans.train(Parseddata, 10, maxIterations=100,runs=100, initializationMode="random")
print str(clusters.clusterCenters)
cc=clusters.clusterCenters
pickle.dump(cc,open("/home/ubuntu/cluster_centers.p","wb"))
print '&&&&&&&&&&&&&&&&&%%%%%%%%%%%%%%%%%',type(cc),'*******************@@@@@@@@@@@@@@@@@@@'
PD1 = sc.textFile("file:///home/ubuntu/music_data_updated_v2.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda p:(p[0].encode('utf-8'),p[1].encode('utf-8'),p[2].encode('utf-8'),p[3].encode('utf-8'),float(p[4]),float(p[5]),float(p[6]),float(p[7]),closestPoint([float(p[4]),float(p[5]),float(p[6]),float(p[7])],cc),p[8].encode('utf-8')))
PD1.saveAsTextFile("file:///home/ubuntu/music_data_clustered_updated_v3.txt")
'''
#PD1_count=PD1.foreach(g)
PD2=sc.textFile("file:///home/ubuntu/music_data.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line:((float(line[3]),float(line[5])),(line[0].encode('utf-8'),line[1].encode('utf-8'))))
PD2_count=PD2.count()
PD3=PD1.rightOuterJoin(PD2)
PD3_count=PD3.count()
PD4=PD3.map(lambda line:(line[1][1][0],line[1][1][1],line[0][0],line[0][1],line[1][0]))
'''

logData = sc.textFile("file:///home/ubuntu/music_data_updated.csv").cache()
sc.stop()
