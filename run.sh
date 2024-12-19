
rm -rf ./output
rm -rf ./unordered_result
mkdir unordered_result

hadoop jar untitled1/target/category-hadoop-1.0-SNAPSHOT.jar

hdfs dfs -get ./output/part-r-00000 ./unordered_result/result.csv
