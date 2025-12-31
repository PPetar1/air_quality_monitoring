#!/bin/bash
   
docker stop openaq-metabase
docker rm openaq-metabase
while read pid; do [[ $(ps -p $pid -o comm= 2>/dev/null) == "airflow" ]] && kill $pid; done < airflow_pids.txt
