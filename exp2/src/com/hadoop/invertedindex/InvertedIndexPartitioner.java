package com.hadoop.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner extends HashPartitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text key, LongWritable value, int numReduceTasks) {
        String term = key.toString().split("@")[0];
        //使用内置的Partition进行划分
        return super.getPartition(new Text(term),value,numReduceTasks);
    }
}
