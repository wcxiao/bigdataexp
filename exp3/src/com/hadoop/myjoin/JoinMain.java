package com.hadoop.myjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinMain {
    public static void main(String []args){
        try{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Join");
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(JoinMapper.class);
            job.setReducerClass(JoinReducer.class);
            job.setPartitionerClass(JoinPartitioner.class);
            job.setJarByClass(JoinMain.class);
            job.setOutputKeyClass(OrderBean.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(OrderBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setGroupingComparatorClass(JoinGroupingComparator.class);
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
