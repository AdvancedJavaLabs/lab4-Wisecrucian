package org.example.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.dto.CategoryDto;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, CategoryDto, Text, CategoryDto>{
    @Override
    protected void reduce(Text key, Iterable<CategoryDto> values, Context context) throws IOException, InterruptedException {
        double totalSum = 0.0;
        double totalQuantity = 0D;

        for (CategoryDto value : values) {
            DoubleWritable sum = value.getTotalSum();
            DoubleWritable quantity = value.getTotalQuantity();

            totalSum += sum.get();
            totalQuantity += quantity.get();
        }

        CategoryDto result = new CategoryDto(totalSum, totalQuantity);

        context.write(key, result);
    }
}
