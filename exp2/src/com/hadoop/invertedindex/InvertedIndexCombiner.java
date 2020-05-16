package com.hadoop.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
            throws IOException, InterruptedException 
        {
        long sum = 0;
        Iterator<LongWritable> it = values.iterator();
        //将一个文件内部相同的单词求和累积起来
        for(;it.hasNext();){
            sum+=it.next().get();
        }
        context.write(key,new LongWritable(sum));
        }
}
