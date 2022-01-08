#!/bin/bash

function run {
  killall monitorUtilization.sh
  /home/fier/textualSimilarity/code/hadoop-2.7.0/bin/hdfs dfs -expunge
  OUTPUTPATH=../data/output.$ALGORITHM.$INPUT.`date +"%Y-%m-%d-%H-%M-%S"`$THETA
  mkdir $OUTPUTPATH

  #copy Hadoop config:
  cp /home/fier/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/mapred-site.xml $OUTPUTPATH
  cp /home/fier/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/hdfs-site.xml $OUTPUTPATH
  cp /home/fier/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/slaves $OUTPUTPATH
  cp /home/fier/textualSimilarity/code/hadoop-2.7.0/etc/hadoop/yarn-site.xml $OUTPUTPATH

  ./monitorUtilization.sh > $OUTPUTPATH/utilization &
  UTILIZATION_PID=`echo $!`

  timeout $TIMEOUT ./runDriverHadoop.sh $ALGORITHM $THETA $INPUT $OUTPUTPATH

  if [ $? -eq 124 ]; then
    /home/fier/textualSimilarity/deployment/killAllMRProcesses.sh
    touch $OUTPUTPATH/terminated.$TIMEOUT
    # Wait a bit, because killing MR processes takes time. Furthermore, the deletion of files in the next step won't work if this is not completely finished:
    sleep 2m
  fi

  kill $UTILIZATION_PID

  mkdir $OUTPUTPATH/OUTPUT
  ../code/hadoop-2.7.0/bin/hdfs dfs -copyToLocal /user/fier/output/* $OUTPUTPATH/OUTPUT
  #WARN hdfs.DFSClient: DFSInputStream has been closed already is caused by an HDFS bug, ignore it:
  #https://issues.apache.org/jira/browse/HDFS-8099

  cat $OUTPUTPATH/OUTPUT/OUTPUT/*/part-r-* > $OUTPUTPATH/consolidatedResults
  cat $OUTPUTPATH/OUTPUT/part-r-* >> $OUTPUTPATH/consolidatedResults
}

# set the line separator:
IFS=$'\n'
for line in $(cat experiment_commands.txt); do 
  IFS=$'\t'
  arr=(${line})
  ALGORITHM=${arr[0]}
  INPUT=${arr[1]}
  THETA=${arr[2]}
  TIMEOUT=${arr[3]}
  run
done;

