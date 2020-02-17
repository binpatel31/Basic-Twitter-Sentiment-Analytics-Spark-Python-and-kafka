from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TkAgg')

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
   
    positive_cnt = []
    negative_cnt = []
    for x in counts:
        if len(x)!= 0:
            if x[0][0]=="positive":
                positive_cnt.append(x[0][1])
                negative_cnt.append(x[1][1])
            else:
                positive_cnt.append(x[1][1])
                negative_cnt.append(x[0][1])
    figure = plt.figure()    
    #Plot labeling and plotting
    plt.plot(positive_cnt,'-bo', label = 'Positive')
    plt.plot(negative_cnt,'-go', label = 'Negative')
    plt.ylabel('Word count')
    plt.xlabel('Time step')
    plt.legend()#loc='upper left')
    figure.savefig("plot.png")	    
    plt.show();
    #plt.show(block=True)

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    f=open(filename,'r')
    words=f.read().splitlines()    
    return words

def func(vals,count):
    return sum(vals,count) if count is not None else sum(vals,0)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    #tweets.pprint()

    words=tweets.flatMap(lambda x: x.split(" "))
    
    dict_positive={}
    dict_negative={}
    for word in pwords:
        dict_positive[word]=1
  
    for word in nwords:
        dict_negative[word]=1

    labeled=words.map(lambda x: ("positive",1) if x in dict_positive else (("negative",1) if x in dict_negative else ("nothing",1)))
   
    filtered=labeled.filter(lambda x: x[0] in ["positive","negative"])
    
    totalcount=filtered.reduceByKey(lambda x,y:x+y)
    #totalcount.pprint()         

    runningCounts=filtered.updateStateByKey(func)
    runningCounts.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    totalcount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    
    ssc.start()  # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
