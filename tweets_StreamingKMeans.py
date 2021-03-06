import sys

import sched,time

from pyspark import  SparkContext
from pyspark.streaming import StreamingContext

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import StreamingKMeans

if __name__=="__main__":
    
    sc=SparkContext(appName="StreamingKMeans")
    ssc=StreamingContext(sc,10)
    
    ssc.checkpoint("file:///tmp/spark")
    
    def parseTrainingData(line):
        cell=line.split(",")
        return Vectors.dense([float(cell[0]),float(cell[1])])
    
    trainingStream=ssc.textFileStream(":/tweets/training")\
                      .map(parseTrainingData)
    
    
    trainingStream.pprint();
    
    model=StreamingKMeans(k=5, decayFactor=1.0).setRandomCenters(2,1.0,0)
    
    print ("Initial centers: "+str(model.latestModel().centers))
    
    model.trainOn(trainingStream)
    
    ssc.start()
    
    s=sched.scheduler(time.time,time.sleep)
    def print_cluster_centers(sc, model):
        print ("Cluster centers: "+ str(model.latestModel().centers))
        s.enter(10,1,print_cluster_centers, (sc,model))
    
    s.enter(10,1,print_cluster_centers, (s,model))
    s.run()
    
    ssc.awaitTermination()
        
        
