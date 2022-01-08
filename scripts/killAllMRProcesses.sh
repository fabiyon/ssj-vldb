#!/bin/bash

#../code/hadoop-2.7.0/bin/mapred job -list

#15/10/12 11:05:22 INFO client.RMProxy: Connecting to ResourceManager at dbis21/192.168.127.21:8032
#Total jobs:2
#                  JobId	     State	     StartTime	    UserName	       Queue	  Priority	 UsedContainers	 RsvdContainers	 UsedMem	 RsvdMem	 NeededMem	   AM info
# job_1444637811402_0009	   RUNNING	 1444640653471	        fier	     default	    NORMAL	              2	              0	   6144M	      0M	     6144M	http://dbis21:8088/proxy/application_1444637811402_0009/
# job_1444637811402_0008	   RUNNING	 1444639479257	        fier	     default	    NORMAL	             29	              1	 231424M	  49152M	   280576M	http://dbis21:8088/proxy/application_1444637811402_0008/

#extrahiert den job-String aus der obigen Ausgabe:
#cat tmp  | grep job_ | cut -f1


../code/hadoop-2.7.0/bin/mapred job -list  | grep job_ | cut -f1 | xargs -L1 ../code/hadoop-2.7.0/bin/mapred job -kill 
