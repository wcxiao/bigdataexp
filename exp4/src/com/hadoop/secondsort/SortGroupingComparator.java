package com.hadoop.secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.net.Inet4Address;

public class SortGroupingComparator extends WritableComparator {
    protected SortGroupingComparator(){
        super(Data.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //return super.compare(a, b);
        Data da = (Data) a;
        Data db = (Data) b;
        return Integer.compare(da.getA(),db.getA());
    }
}
