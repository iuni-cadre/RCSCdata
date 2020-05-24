ssh iuni2.carbonate.uits.iu.edu
cd /N/slate/yan30/COVID
module swap python/3.6.8
source activate tf-prob

export HADOOP_USER_NAME=hdfs
export PATH=~/.local/bin:$PATH

###### now you should be able to run the command line ######
pyspark
exit()

spark-shell
:q

###### Setting up jupyter server #####

pip install --user Toree
jupyter toree install --spark_home=$SPARK_HOME --user --spark_opts=" --master yarn  --packages com.databricks:spark-xml_2.11:0.5.0,graphframes:graphframes:0.7.0-spark2.4-s_2.11 --driver-memory 8G --executor-memory 14G --executor-cores 7 --conf spark.driver.maxResultSize=8g"
jupyter toree install --spark_home=$SPARK_HOME --user --spark_opts=" --master yarn --deploy-mode client --jars /opt/cloudera/parcels/CDH/jars/commons-dbcp-1.4.jar,/opt/cloudera/parcels/CDH/jars/datanucleus-api-jdo-4.2.5.jar,/opt/cloudera/parcels/CDH/jars/datanucleus-core-4.1.17.jar,/opt/cloudera/parcels/CDH/jars/datanucleus-rdbms-4.1.17.jar --packages com.databricks:spark-xml_2.11:0.5.0 --driver-memory 8G --executor-memory 14G --executor-cores 7 --conf spark.driver.maxResultSize=8g"
jupyter kernelspec list

hadoop fs -rm -R URI
hdfs dfs -mkdir /data1
hdfs dfs -copyFromLocal /wos_17_18/ /data1/WoSraw/
hdfs dfs -ls /
hdfs dfs -copyToLocal <hdfs_input_file_path> <output_path>

jupyter serverextension enable --py jupyterlab
cd /N/project/iuni_cadre
jupyter notebook --no-browser --port=8000 --ip=149.165.230.163

spark.sparkContext.uiWebUrl
sqlContext.sql("set spark.sql.caseSensitive=true") 
