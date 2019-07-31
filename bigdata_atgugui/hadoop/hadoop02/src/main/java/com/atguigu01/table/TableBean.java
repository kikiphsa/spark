package com.atguigu01.table;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class TableBean implements Writable {

    private String order_id;
    private String p_id;
    private int amnout;
    private String pname;
    private String flag;// 区分订单表（0）和产品表（1）

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(p_id);
        out.writeInt(amnout);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readUTF();
        p_id = in.readUTF();
        amnout = in.readInt();
        pname = in.readUTF();
        flag = in.readUTF();

    }

    public TableBean() {
    }

    public TableBean(String order_id, String p_id, int amnout, String pname, String flag) {
        this.order_id = order_id;
        this.p_id = p_id;
        this.amnout = amnout;
        this.pname = pname;
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amnout;
    }

    public void setAmount(int amnout) {
        this.amnout = amnout;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return order_id + "\t" + pname + "\t" + amnout;
    }
}
