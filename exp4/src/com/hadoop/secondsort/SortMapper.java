package com.hadoop.secondsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, Data, NullWritable> {
    private Data data;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        data = new Data();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String[] fields = value.toString().split("\t");
        data.setA(Integer.parseInt(fields[0]));
        data.setB(Integer.parseInt(fields[1]));
        context.write(data,NullWritable.get());
    }
}
