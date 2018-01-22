# _ZR

## Running the system
_/Applications/anaconda/bin/spark-submit stream_analyze.py -i ../data/file2.txt -debug -partitions 1 -poi "../input/poi*.txt"_

From within the pyspark folder

## Running unit tests
_python test/test_stream_analyze.py_

## Interesting stuff
Can read multiple POI (path of interest) files (according to pattern for matching files) and creates a list of user matching each of the POI

### Options
usage: stream_analyze.py [-h] [-i INPUTFILES] [-poi POIFILES] [-printHeader]
                         [-partitions PARTITIONS] [-debug] [-printInfo]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUTFILES, --inputFiles INPUTFILES
                        Location of input stream files
  -poi POIFILES, --poiFiles POIFILES
                        CSV locations of Path Of Interest files
  -printHeader, --printHeader
                        print header in the beginning of output files
  -partitions PARTITIONS, --partitions PARTITIONS
                        Repartition each outut to defined number of partitions
  -debug, --debug       Should print debug info
  -printInfo, --printInfo
                        Should print INFO level messages
