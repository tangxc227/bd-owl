package com.owl.hadoop.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:10 2018/12/6
 * @Modified by:
 */
public class PairWritable implements WritableComparable<PairWritable> {

    private String first;
    private int second;

    public PairWritable() {

    }

    public PairWritable(String first, int second) {
        set(first, second);
    }

    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.first);
        out.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }

    @Override
    public int compareTo(PairWritable o) {
        if (this == o) {
            return 0;
        }
        int tmp = this.first.compareTo(o.first);
        if (0 != tmp) {
            return tmp;
        }
        return Integer.compare(this.second, o.second);
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairWritable that = (PairWritable) o;
        return second == that.second &&
                Objects.equals(first, that.first);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

}
