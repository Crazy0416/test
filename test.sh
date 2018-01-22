#! /bin/sh

rm -rf ./EquiJoin*.class
rm ./EquiJoin.jar
hdfs dfs -rm -R /outputMinho

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
echo rm Finish
hadoop com.sun.tools.javac.Main EquiJoin.java
jar cf EquiJoin.jar EquiJoin*.class

hadoop jar EquiJoin.jar EquiJoin /input/records.json /outputMinho
