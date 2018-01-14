import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as fn

def parse_args():
    argumentParser = argparse.ArgumentParser()
    #input parameters
    argumentParser.add_argument("-i", "--inputFiles", help="Location of input stream files", default="./", type=str)
    argumentParser.add_argument("-debug", "--debug", help="Should print debug info", default=False, action="store_true")
    
    #general parameters
    
    return argumentParser.parse_args()


def extractConversionsPerUser(userConversions):    
    userConversionsCounts = userConversions.count()
    userConversionsCounts.show()
    return userConversions


def extractActionsTillConversion(arguments, df, userConversions):
    userFirstConversion = userConversions.agg(fn.min('timestamp').alias('firstConversion'))
    userFirstStart = df.filter(df.type == 'start_session').groupBy('user_id').agg(fn.min('timestamp').alias('firstStartSession'))
    actionsTillConversion = df.join(userFirstStart.join(userFirstConversion, 'user_id'), 'user_id')
    if arguments.debug:
        actionsTillConversion.show()
    userShortestConversion = actionsTillConversion.filter(actionsTillConversion.timestamp >= actionsTillConversion.firstStartSession).filter(actionsTillConversion.timestamp < actionsTillConversion.firstConversion).groupBy('user_id').count().sort(fn.desc('count'))
    userShortestConversion.show()
    avgConvertingDistance = userShortestConversion.agg(fn.avg('count').alias('avg_converting_distance'))
    avgConvertingDistance.show()

def pathMatch(path):
    l = len(pathOfInterest)
    return any(pathOfInterest == path[offset:offset+l] for offset in range(1 + len(path) - l))

def extractUsersMatchingPath(df):
    userPaths = df.orderBy('timestamp').groupBy('user_id').agg(fn.collect_list('url').alias('path'))
    udfPathMatch = fn.udf(pathMatch)
    userPaths.filter(udfPathMatch('path') == True).show()


def readInput(arguments, spark):
    rd = spark.sparkContext.textFile(arguments.inputFiles).map(lambda line:line.split("\t"))
    invalidCount = rd.filter(lambda x:len(x) != 4).count()
    print "Num of invalid rows: ", invalidCount
    rd = rd.filter(lambda x:len(x) == 4).map(lambda (url, user_id, timestamp, type):(url, user_id, eval(timestamp), type))
    if arguments.debug:
        print rd.collect()[:10]
    #Process to dataframe
    df = spark.createDataFrame(rd).toDF('url', 'user_id', 'timestamp', 'type')
    return df

global pathOfInterest

def main():
    #process input
    arguments = parse_args()
    
    #get context
    spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()
    
    #process input
    df = readInput(arguments, spark)
    if arguments.debug:
        df.show()
    
    
    #Get users matching path of urls
    global pathOfInterest
    pathOfInterest = ['/jobs','/jobs/fujifilm-26978f85/software-engineering-intern-temporary-d040c9c4']
    extractUsersMatchingPath(df)
    userConversions = df.filter(df.type == 'conversion').groupBy('user_id')
    #Get num conversions per user
    extractConversionsPerUser(userConversions)
    #Get actions till conversion per user and average
    extractActionsTillConversion(arguments, df, userConversions)
    
    #pattern_of_interest = fn.array(*[fn.lit(x) for x in pathOfInterest])
    #usersWithMatchingPath = userPaths.filter(userPaths.path == pattern_of_interest)
    #usersWithMatchingPath.select('user_id').show()
    
    
    #print summary


if __name__ == "__main__":
    main()