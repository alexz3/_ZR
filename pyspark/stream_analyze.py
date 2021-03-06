import argparse
import glob
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
import pyspark.sql.functions as fn
from utils import *

def parse_args():
    argumentParser = argparse.ArgumentParser()
    #input parameters
    argumentParser.add_argument("-i", "--inputFiles", help="Location of input stream files", default="./", type=str)
    argumentParser.add_argument("-poi", "--poiFiles", help="CSV locations of Path Of Interest file", default=None, type=str)
    #output parameters
    argumentParser.add_argument("-printHeader", "--printHeader", help="print header in the beginning of output files", default=False, action="store_true")
    argumentParser.add_argument("-partitions", "--partitions", help="Repartition each outut to defined number of partitions", default=None, type=int)
    #run settings parameters
    argumentParser.add_argument("-debug", "--debug", help="Should print debug info", default=False, action="store_true")
    argumentParser.add_argument("-printInfo", "--printInfo", help="Should print INFO level messages", default=False, action="store_true")    
    
    return argumentParser.parse_args()

def extractTopConvertingUsers(df, n):  
    return df.filter(df.type == 'conversion')\
    .groupBy('user_id')\
    .count()\
    .sort(fn.desc('count'))\
    .limit(n)  

def minConversion(path):
    i=0
    for p in path:
        if p['type']=='conversion':
            return i
        i+=1
    return -1

def extractMinConversion(dfSessionized):
    udfMinConversion = fn.udf(minConversion)
    return dfSessionized.select('user_id', udfMinConversion('path').alias('conversion_distance'))\
    .filter(fn.col('conversion_distance') >= 0)\
    .groupBy('user_id')\
    .agg(fn.min('conversion_distance').alias('conversion_distance'))\
    .sort(fn.desc('conversion_distance'))

def pathMatch(colPath):
    #Receiving column of path (list of structs with URLs of interest in 'url' field)
    path = [p['url'] for p in colPath]
    l = len(pathOfInterest)
    return any(pathOfInterest == path[offset:offset+l] for offset in range(1 + len(path) - l))

#separate method extractSessionsMatchingPath for easier unit test
def extractSessionsMatchingPath(dfSessionized):
    udfPathMatch = fn.udf(pathMatch)
    return dfSessionized.filter(udfPathMatch('path') == True)

def extractUsersMatchingPath(dfSessionized):
    return extractSessionsMatchingPath(dfSessionized)\
    .select('user_id')\
    .distinct()

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


def init(arguments):
    #get context
    spark = SparkSession.builder.appName("PythonZR").getOrCreate()
    if not arguments.printInfo:
        quietLogs(spark)
    return spark

def main():
    #process input
    arguments = parse_args()
    
    #initialize spark
    spark = init(arguments)
    
    #process input
    df = readInput(arguments, spark)
    if arguments.debug:
        print lineno()
        df.show()
    
    #Get num conversions per user
    top10ConvertingUsers = extractTopConvertingUsers(df, 10)
    if arguments.debug:
        print top10ConvertingUsers
    writeDataframe('q1_top10ConvertingUsers', top10ConvertingUsers, arguments.printHeader, arguments.partitions)
    
    #sessionize
    #TODO translate type to numbers so that start session will precede other actions
    windowval = Window.partitionBy('user_id').orderBy('timestamp').rangeBetween(Window.unboundedPreceding, 0)
    dfSessionized = df.withColumn('session_id', fn.sum(fn.when(df["type"] == 'start_session', 1).otherwise(0)).over(windowval))\
    .groupBy('user_id','session_id')\
    .agg(fn.collect_list(fn.struct('type', 'url','timestamp')).alias('path'))
    if arguments.debug:
        print lineno()
        dfSessionized.show(100)
    
    convertionDistancePerUser = extractMinConversion(dfSessionized)
    if arguments.debug:
        print lineno()
        convertionDistancePerUser.show(100)
    writeDataframe('q2_conversionDistancePerUser', convertionDistancePerUser, arguments.printHeader, arguments.partitions)
    avgConvserionDistance = convertionDistancePerUser.agg(fn.avg('conversion_distance').alias('avg_converting_distance'))
    writeDataframe('q3_avgConversionDistance', avgConvserionDistance, arguments.printHeader, arguments.partitions)
    
    if arguments.poiFiles is not None:
        global pathOfInterest
        for poiPath in arguments.poiFiles.split(','):
            if arguments.debug:
                print 'Processing path ', poiPath
            for filePath in glob.glob(poiPath):
                if arguments.debug:
                    print 'Processing file ', filePath
                pathOfInterest = readFileToList(filePath)
                #Get users matching path of urls
                patternMatchingUsers = extractUsersMatchingPath(dfSessionized)
                if arguments.debug:
                    print lineno()
                    patternMatchingUsers.show()
                writeDataframe('q4_patternMatchingUsers/'+filePath, patternMatchingUsers, arguments.printHeader, arguments.partitions)
    
    #print summary


if __name__ == "__main__":
    main()