package com.hadoop.secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortPartitioner extends HashPartitioner<Data, NullWritable> {
    @Override
    public int getPartition(Data key, NullWritable value, int numReduceTasks) {
        //return super.getPartition(key, value, numReduceTasks);
        return (key.getA() & 2147483647) % numReduceTasks;
    }
}
