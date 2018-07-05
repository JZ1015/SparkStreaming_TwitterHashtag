import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__=='__main__':
    sc=SparkContext(appName='StreamingWordCount')
    ssc=StreamingContext(sc,2)
    
    ssc.checkpoint('file:///tmp/spark')
    
    lines=ssc.socketTextStream('localhost',7777)
    
    def countWords(newValues,lastSum):
        if lastSum is None:
            lastSum=0
        return sum(newValues,lastSum)
    
    def BTSTrend(w):
        if 'BTS' in w and (w.startswith('#') or w.startswith('@') ):
            return w
        
    
    word_counts= lines.flatMap(lambda line:line.split(' '))\
                 .filter(lambda w:BTSTrend(w))\
                 .map(lambda word: (word,1))\
                 .reduceByKeyAndWindow(lambda a,b: a+b,lambda a,b:a-b,60,2,None,lambda (k,v):v>0)\
                 .map(lambda (k,v):(v,k))
    
    sortedTopCounts=word_counts.transform(lambda rdd :rdd.sortByKey(False))
    
    
    sortedTopCounts.pprint(5)
    
    ssc.start()
    ssc.awaitTermination()