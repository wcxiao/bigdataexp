package com.hadoop.invertedindex;

//import org.apache.commons.configuration.Configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedIndex {

    public static void main(String []args){
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"Inverted Index");
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setPartitionerClass(InvertedIndexPartitioner.class);
            job.setJarByClass(InvertedIndex.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job,new Path(args[1]));
            FileOutputFormat.setOutputPath(job,new Path(args[2]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
