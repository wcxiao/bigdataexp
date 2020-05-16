package com.hadoop.secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortReducer extends Reducer<Data, NullWritable, Data, NullWritable> {
    @Override
    protected void reduce(Data key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        while (iterator.hasNext()) {
            context.write(key, NullWritable.get());
            iterator.next();
        }
    }
}
