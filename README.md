# MapReduce

This is the code to the paper "Set Similarity Joins on MapReduce: An Experimental Survey" accepted for VLDB 2018 (http://www.vldb.org/pvldb/vol11/p1110-fier.pdf):
* ClusterJoin: de.huberlin.clusterjoin
* MRGroupJoin: de.huberlin.textualsimilarityhadoop.GroupJoinJob
* FullFilteringJoin: de.huberlin.textualsimilarityhadoop.ElsayedJob
* MGJoin: de.huberlin.mgjoin
* MassJoin: de.huberlin.massjoin
* MRSimJoin: de.huberlin.textualsimilarityhadoop.MRSimJoinJob
* SSJ-2R: de.huberlin.textualsimilarityhadoop.SSJ2RNewJob
* VernicaJoin: de.huberlin.vernicajoin
* V-SMART: de.huberlin.vsmart
* FS-Join: de.huberlin.textualsimilarityhadoop.FSJoinJob

Folder structure:
* dist: contains compiled JAR
* scripts: call scripts
* src: Java source code

## Compilation
The code has been compiled and executed with Hadoop 2.7.0 (Yarn). 

## Input File Requirements
* Record line: RecordId TAB tokensCommaSeparated

## Usage
./scripts/runExperimentsWithDriver.sh executes all experiments listed in experiment_commands.txt. It monitors the number of nodes concurrently used (monitorUtilization.sh). It enforces timeouts set in the experiment_commands.txt and cleans up if a process hangs. 
