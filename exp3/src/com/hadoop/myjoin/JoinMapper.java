package com.hadoop.myjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
//public class JoinMapper extends Mapper<LongWritable, Text,String, OrderBean> {
    private String filename;
    OrderBean orderBean;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        filename = fs.getPath().getName();
        orderBean = new OrderBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if (filename.equals("product.txt")) {
            orderBean.setTag(-1);
            orderBean.setPid(Integer.parseInt(fields[0]));
            orderBean.setPname(fields[1]);
            orderBean.setPrice(fields[2]);
        }else if(filename.equals("order.txt")){
            orderBean.setTag(1);
            orderBean.setOid(Integer.parseInt(fields[0]));
            orderBean.setOdata(fields[1]);
            orderBean.setPid(Integer.parseInt(fields[2]));
            orderBean.setOamount(fields[3]);
        }
        context.write(orderBean,NullWritable.get());
        //context.write(orderBean.getPid(),orderBean);
    }
}
