#!/bin/bash

while [ true ]; do
# sed '1,2d' löscht die ersten beiden Zeilen
# die 7. Spalte ist die UsedContainers-Spalte.
# Möglicherweise werden aber mehrere Werte dargestellt, wenn mehrere Programme laufen...
  numberOfUsedContainers=`/home/fier/textualSimilarity/code/hadoop-2.7.0/bin/mapred job -list 2> /dev/null | sed '1,2d' | cut -f7`
  echo `date +"%D %T"` $numberOfUsedContainers
  sleep 1
done
