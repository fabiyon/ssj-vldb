#!/bin/bash

ALGORITHM=$1
THETA=$2
INPUT=$3
OUTPUTPATH=$4

# -m: MRSimJoin: maximum memory before split in MB; V-SMART: max number of records in one chunk
# -n: ClusterJoin: number of pivots
# -s: ClusterJoin: number of samples
# -d: modulo (war zunächst beim ClusterJoin, wird dort aber nicht mehr verwendet)
# -p: MRSimJoin & ClusterJoin: number of partitions in each iteration/ partition size
# -e true: End-To-End
IFS=$'\n'
for line in $(cat "callScripts/$ALGORITHM"); do 
  ../code/hadoop-2.7.0/bin/hadoop jar ../code/TextualSimilarityHadoop1/dist/TextualSimilarityHadoop1.jar de.huberlin.$ALGORITHM $line -t $THETA -m 10000 -n 3 -d 20 -p 20000 -s 50000 -i hdfs:///user/fier/$INPUT -o hdfs:///user/fier/output >> $OUTPUTPATH/hadoop-stdout 2>&1
done;
