package org.example.dto;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryQuantityDto implements Writable {
    private Text category = new Text();
    private DoubleWritable totalQuantity = new DoubleWritable();

    public CategoryQuantityDto() {}

    public CategoryQuantityDto(String totalSum, double totalQuantity) {
        this.category = new Text(totalSum);
        this.totalQuantity  = new DoubleWritable(totalQuantity);
    }

    public Text getCategory() {
        return category;
    }

    public DoubleWritable getTotalQuantity() {
        return totalQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.category.write(out);
        this.totalQuantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.category.readFields(in);
        this.totalQuantity.readFields(in);
    }

    @Override
    public String toString() {
        return category + "\t" + totalQuantity;
    }
}