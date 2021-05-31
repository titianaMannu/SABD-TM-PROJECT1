#!/bin/bash


cleanup() {
	if [[ "${HDFS_MODE}" == "master" ]]; then 
		$HADOOP_HDFS_HOME/sbin/stop-dfs.sh;
	fi
}

#Trap SIGTERM
trap 'cleanup' SIGTERM;



if [[ -n "${HDFS_WORKERS}" ]]; then
	IFS=',' read -ra WORKERS <<< "${HDFS_WORKERS}"
	for worker in "${WORKERS[@]}"; do
		echo $worker >> $HADOOP_HOME/etc/hadoop/workers;
	done
fi


if [[ "${HDFS_MODE}" == "worker" ]]; then
	echo "Starting worker node";
	service ssh start;
elif [[ "${HDFS_MODE}" == "master" ]]; then
	echo "Starting master node";
	service ssh start;
	echo "N" | hdfs namenode -format;
	$HADOOP_HDFS_HOME/sbin/start-dfs.sh;
	hdfs dfs -mkdir /Results
	hdfs dfs -mkdir /Results/query1
	hdfs dfs -mkdir /Results/query2
	hdfs dfs -chmod 777 /Results
	hdfs dfs -chmod 777 /Results/query1
	hdfs dfs -chmod 777 /Results/query2
	echo "Master node is up and running"
else 
	echo "HDFS_MODE is not correctly set";
	exit 1;
fi

while true; do sleep 1000; done

#Wait
wait $!;
