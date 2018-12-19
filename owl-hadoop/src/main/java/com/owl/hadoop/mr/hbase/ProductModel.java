package com.owl.hadoop.mr.hbase;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 08:53 2018/12/19
 * @Modified by:
 */
public class ProductModel implements WritableComparable<ProductModel> {

    private String id;
    private String name;
    private String price;

    public ProductModel() {
    }

    public ProductModel(String id, String name, String price) {
        this.set(id, name, price);
    }

    public void set(String id, String name, String price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeUTF(this.name);
        out.writeUTF(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.price = in.readUTF();
    }

    @Override
    public int compareTo(ProductModel o) {
        if (this == o) {
            return 0;
        }
        int tmp = this.id.compareTo(o.id);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.name.compareTo(o.name);
        if (0 != tmp) {
            return tmp;
        }
        return this.price.compareTo(o.price);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductModel that = (ProductModel) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(price, that.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, price);
    }

}
