#import org.apache.log4j.Logger
#import org.apache.log4j.Level
from inspect import currentframe

def quietLogs(sc):
    #Logger.getRootLogger().setLevel(Level.WARN)
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

def lineno():
    return currentframe().f_back.f_lineno

def writeDataframe(folder, df, printHeader = True, repartitions = None):
    if repartitions is not None:
        df = df.repartition(repartitions)
    df.write.format("com.databricks.spark.csv")\
    .mode('overwrite')\
    .option("header", printHeader)\
    .save(folder)

def readFileToList(filePath):
    with open(filePath) as f:
        lines = f.readlines()
    return [x.strip() for x in lines] 