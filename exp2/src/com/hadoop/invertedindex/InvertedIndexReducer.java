package com.hadoop.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.misc.GThreadHelper;

import java.io.IOException;
import java.util.*;

public class InvertedIndexReducer extends Reducer<Text, LongWritable,Text,Text> {
    private String t_prev;//当前词语名
    private int showFile;//当前词语出现的文件计数
    private long showTime;//当前词语计数
    private Text postingList;
    private String outputList;
    @Override
    protected void setup(Context context) 
                throws IOException, InterruptedException {
        t_prev = "";
        showFile =0;
        showTime =0;
        postingList = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
                throws IOException, InterruptedException {
        String keys[] = key.toString().split("@");
        String term = keys[0];
        //检查当前单词名与下一个读入的单词名，如果相同且非空，则进行输出操作，否则继续执行
        if((!term.equals(t_prev)) && t_prev!=""){
            // 计算平均值并配置输出
            double ave = (double)showTime/showFile;
            String tmp = String.format("%.2f", ave) + ",";
            outputList = tmp + outputList;
            postingList.set(outputList);
            //通过context输出结果
            context.write(new Text(t_prev),postingList);
            postingList = new Text();
            outputList = "";
            showTime =0;
            showFile =0;
        }
        long cnt = 0;
        Iterator<LongWritable> it = values.iterator();
        //累计得到一个文件中莫个词语的总值值
        while(it.hasNext()){
            cnt+=it.next().get();
        }
        //累加多个文件中某个词语出现的次数以及出现该词语的文件总数
        showTime += cnt;
        showFile += 1;
        //记录单个文件中某个词语的情况到输出序列中
        String tmp = keys[1] + ":" + cnt + ";";
        outputList =outputList +tmp;
        t_prev = term;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double ave = (double)showTime/showFile;
        String tmp = String.format("%.2f", ave) + ",";
        outputList = tmp + outputList;
        postingList.set(outputList);
        context.write(new Text(t_prev),postingList);
    }
}



