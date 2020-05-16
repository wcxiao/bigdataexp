package com.hadoop.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Data implements WritableComparable<Data> {
    private int a;
    private int b;

    public Data(){
        a = -1;
        b = -1;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }

    public void setA(int a) {
        this.a = a;
    }

    public void setB(int b) {
        this.b = b;
    }

    @Override
    public int compareTo(Data data) {
        if(a == data.a){
            return (-Integer.compare(b,data.b));
        }else{
            return Integer.compare(a,data.a);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(a);
        dataOutput.writeInt(b);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.a = dataInput.readInt();
        this.b = dataInput.readInt();
    }

    /*@Override
    public String toString() {
        return "Data{" +
                "a=" + a +
                ", b=" + b +
                '}';
    }*/

    @Override
    public String toString() {
        return a + "\t" + b;
    }
}
