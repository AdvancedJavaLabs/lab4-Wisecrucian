package org.example.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.dto.CategoryDto;
import org.example.dto.CategoryQuantityDto;

import java.io.IOException;

public class SortingReducer extends Reducer<DoubleWritable, CategoryQuantityDto, Text, CategoryDto> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<CategoryQuantityDto> values, Context context) throws IOException, InterruptedException {
        CategoryQuantityDto data = values.iterator().next();
        Text category = data.getCategory();
        DoubleWritable quantity = data.getTotalQuantity();
        CategoryDto result = new CategoryDto(key.get(), quantity.get());

        context.write(category, result);
    }
}