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
jupyter kernelspec list

hadoop fs -rm -R URI
hdfs dfs -mkdir /data1
hdfs dfs -copyFromLocal /wos_17_18/ /data1/WoSraw/
hdfs dfs -ls /
hdfs dfs -copyToLocal <hdfs_input_file_path> <output_path>

jupyter serverextension enable --py jupyterlab
jupyter notebook --no-browser --port=8000 --ip=149.165.230.163 ## With the ip option, it is only accessible to other iuni nodes
## if you log in with red desktop or other karst/carbonate notes
ssh -t -t yan30@iuni2.carbonate.uits.iu.edu -L 8001:localhost:8001

spark.sparkContext.uiWebUrl
sqlContext.sql("set spark.sql.caseSensitive=true") 
