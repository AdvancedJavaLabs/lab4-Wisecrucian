
rm -rf ./output_unordered
rm -rf ./final_output
mkdir final_output

hadoop jar sorting/target/sorting-1.0-SNAPSHOT.jar

hdfs dfs -get ./output_unordered/part-r-00000 ./final_output/result.csv
