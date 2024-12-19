package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.dto.CategoryQuantityDto;
import org.example.hadoop.SortingMapper;
import org.example.hadoop.SortingReducer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String inputDir = "/Users/max/Downloads/ITMO/Parallels/Labs/lab4-Wisecrucian/unordered_result";
        String outputDir = "/Users/max/Downloads/ITMO/Parallels/Labs/lab4-Wisecrucian/output_unordered";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Sorting");

        job.setJarByClass(Main.class);

        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(CategoryQuantityDto.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        if (job.waitForCompletion(true)) {
            System.exit(0);
        }
    }
}