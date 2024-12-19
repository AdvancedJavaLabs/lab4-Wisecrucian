package org.example.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.dto.CategoryQuantityDto;

import java.io.IOException;

public class SortingMapper extends Mapper<Object, Text, DoubleWritable, CategoryQuantityDto> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 3) {
            String categoryName = fields[0];
            double sum = Double.parseDouble(fields[1]);
            double quantity = Double.parseDouble(fields[2]);

            context.write(new DoubleWritable(sum), new CategoryQuantityDto(categoryName, quantity));
        }
    }
}