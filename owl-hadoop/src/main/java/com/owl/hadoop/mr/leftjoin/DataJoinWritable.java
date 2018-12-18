package com.owl.hadoop.mr.leftjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:45 2018/12/7
 * @Modified by:
 */
public class DataJoinWritable implements Writable {

    private String tag;
    private String data;

    public DataJoinWritable() {

    }

    public DataJoinWritable(String tag, String data) {
        set(tag, data);
    }

    public void set(String tag, String data) {
        this.tag = tag;
        this.data = data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.tag);
        out.writeUTF(this.data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tag = in.readUTF();
        this.data = in.readUTF();
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataJoinWritable that = (DataJoinWritable) o;
        return Objects.equals(tag, that.tag) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, data);
    }

}
