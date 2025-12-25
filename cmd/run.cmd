@echo off
setlocal

set APP_JAR=D:\ideaSource\spark-demos\target\spark-demos-1.0.0.jar
set MAIN_CLASS=com.github.liyibo1110.spark.sql.JsonDataSourceDemo
set LOG4J_CONF=D:\ideaSource\spark-demos\src\main\resources\log4j2.xml

set HADOOP_CONF_DIR=D:\software\hadoop3\linux-config
set YARN_CONF_DIR=D:\software\hadoop3\linux-config

set HDFS_NAMENODE=hdfs://master:9000
set YARN_RM=master:8032
set SPARK_HISTORY=hdfs://master:9000/spark-history

set EXEC_MEM=1G
set EXEC_CORES=1
set NUM_EXEC=1

call D:\software\spark3\bin\spark-submit.cmd ^
  --class %MAIN_CLASS% ^
  --master yarn ^
  --deploy-mode cluster ^
  --files %LOG4J_CONF% ^
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=log4j2.xml ^
  --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=log4j2.xml ^
  --conf spark.hadoop.fs.defaultFS=%HDFS_NAMENODE% ^
  --conf spark.hadoop.yarn.resourcemanager.address=%YARN_RM% ^
  --conf spark.eventLog.enabled=true ^
  --conf spark.eventLog.dir=%HDFS_NAMENODE%/spark-history ^
  --conf spark.history.ui.port=18080 ^
  --executor-memory %EXEC_MEM% ^
  --executor-cores %EXEC_CORES% ^
  --num-executors %NUM_EXEC% ^
  %APP_JAR%

endlocal
pause