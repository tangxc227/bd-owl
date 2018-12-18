package com.owl.hadoop.mr.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.io.Writable;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:38 2018/12/18
 * @Modified by:
 */
public interface MongoDBWritable extends Writable {

    /**
     * 从MongoDB中读取数据
     *
     * @param dbObject
     */
    void readFields(DBObject dbObject);

    /**
     * 往MongoDB中写入数据
     *
     * @param dbCollection
     */
    void write(DBCollection dbCollection);

}
