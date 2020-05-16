package com.hadoop.myjoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    Integer tag;//order is 1, product is -1
    private Integer oid;
    private String odata;
    private Integer pid;
    private String oamount;
    private String pname;
    private String price;

    public OrderBean() {
        this.tag = 0;
        this.oid = 0;
        this.oamount = "";
        this.odata = "";
        this.pid = 0;
        this.pname = "";
        this.price = "";
    }

    public OrderBean(Integer tag, Integer oid, String odata, Integer pid, String oamount, String pname, String price) {
        this.tag = tag;
        this.oid = oid;
        this.oamount = oamount;
        this.odata = odata;
        this.pid = pid;
        this.pname = pname;
        this.price = price;
    }

    public Integer getTag() {
        return tag;
    }

    public Integer getPid() {
        return pid;
    }

    public String getPname() {
        return pname;
    }

    public String getPrice() {
        return price;
    }

    public void setTag(Integer tag) {
        this.tag = tag;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public void setOamount(String oamount) {
        this.oamount = oamount;
    }

    public void setOdata(String odata) {
        this.odata = odata;
    }

    public void setOid(Integer oid) {
        this.oid = oid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    @Override
    public int hashCode() {
        return this.getPid();
    }

    @Override
    public int compareTo(OrderBean orderBean) {//溢出 and merge
        if(this.pid.equals(orderBean.pid)){
            if(this.tag.equals(orderBean.tag)){
                return this.oid.compareTo(orderBean.oid);
            }
            else
                return this.tag.compareTo(orderBean.tag);
        }
        else
            return this.pid.compareTo(orderBean.pid);
            
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.tag);
        dataOutput.writeUTF(Integer.toString(this.oid));
        dataOutput.writeUTF(this.oamount);
        dataOutput.writeUTF(this.odata);
        dataOutput.writeUTF(Integer.toString(this.pid));
        dataOutput.writeUTF(this.pname);
        dataOutput.writeUTF(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = dataInput.readInt();
        this.oid = Integer.parseInt(dataInput.readUTF());
        this.oamount = dataInput.readUTF();
        this.odata = dataInput.readUTF();
        this.pid = Integer.parseInt(dataInput.readUTF());
        this.pname = dataInput.readUTF();
        this.price = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return String.valueOf(oid) + ' ' + odata + ' ' + String.valueOf(pid) + ' ' + pname + ' ' + price + ' ' + oamount;
    }

//    @Override
//    public String toString() {
//        return "OrderBean{" +
//                "tag=" + tag +
//                ", oid='" + oid + '\'' +
//                ", odata='" + odata + '\'' +
//                ", pid='" + pid + '\'' +
//                ", oamount='" + oamount + '\'' +
//                ", pname='" + pname + '\'' +
//                ", price='" + price + '\'' +
//                '}';
//    }
}
