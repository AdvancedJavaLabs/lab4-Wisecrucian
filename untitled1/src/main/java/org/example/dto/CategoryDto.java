package org.example.dto;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryDto extends IntWritable implements Writable {
    private DoubleWritable totalSum = new DoubleWritable();
    private DoubleWritable totalQuantity = new DoubleWritable();

    public CategoryDto() {}

    public CategoryDto(double totalSum, double totalQuantity) {
        this.totalSum = new DoubleWritable(totalSum);
        this.totalQuantity  = new DoubleWritable(totalQuantity);
    }

    public DoubleWritable getTotalSum() {
        return totalSum;
    }

    public DoubleWritable getTotalQuantity() {
        return totalQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.totalSum.write(out);
        this.totalQuantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.totalSum.readFields(in);
        this.totalQuantity.readFields(in);
    }

    @Override
    public String toString() {
        return totalSum + "\t" + totalQuantity;
    }
}