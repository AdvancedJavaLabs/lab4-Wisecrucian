package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.dto.CategoryDto;
import org.example.hadoop.SalesMapper;
import org.example.hadoop.SalesReducer;

import java.util.Scanner;


public class Main {
    public static void main(String[] args) throws Exception {
        String inputDir = "/Users/max/Downloads/ITMO/Parallels/Labs/lab4-Wisecrucian/input";
        String outputDir = "/Users/max/Downloads/ITMO/Parallels/Labs/lab4-Wisecrucian/output";

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter reducers count: ");
        int numReducers = scanner.nextInt();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Analysis");

        job.setJarByClass(Main.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setNumReduceTasks(numReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CategoryDto.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        long startTime = System.currentTimeMillis();

        if (job.waitForCompletion(true)) {
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;

            System.out.println("Execution time: " + duration + " ms");
            System.exit(0);
        }
    }
}