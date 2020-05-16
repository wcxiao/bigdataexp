package com.hadoop.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //取得输入文件的划分
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName=fileSplit.getPath().getName();
        //去掉文件名的后缀
        fileName = fileName.substring(0,fileName.lastIndexOf("."));
        fileName = fileName.substring(0,fileName.lastIndexOf("."));

        Text word = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString());
        for(;itr.hasMoreTokens();){
            //word即为：词语@文件名
            word.set(itr.nextToken()+"@"+fileName);
            //Context：收集Mapper输出的<k,v>对。
            context.write(word,new LongWritable(1));
        }
    }
}
