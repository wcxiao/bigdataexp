package com.hadoop.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortMain {
    public static void main(String []args){
        try{
            Configuration conf = new Configuration();
            Job job = new Job(conf,"SecondSort");
            //job.setNumReduceTasks(4);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);
            job.setPartitionerClass(SortPartitioner.class);
            job.setJarByClass(SortMain.class);
            job.setOutputKeyClass(Data.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(Data.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setGroupingComparatorClass(SortGroupingComparator.class);
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
