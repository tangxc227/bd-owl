package com.owl.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Author: tangxc
 * @Description: 自定义UDF，要求继承udf，并重载实现evaluate方法<br/>
 * @Date: Created in 09:58 2018/12/19
 * @Modified by:
 */
public class UDFLowerOrUpperCase extends UDF {

    /**
     * 转换小写
     *
     * @param value
     * @return
     */
    public Text evaluate(Text value) {
        return evaluate(value, "lower");
    }

    /**
     * 对参数t进行大小写转换
     *
     * @param value
     * @param lowerOrUpper 如果该值为lower，则进行小写转换，如果该值为upper则进行大写转换，其他情况不进行转换。
     * @return
     */
    public Text evaluate(Text value, String lowerOrUpper) {
        if (value == null) {
            return value;
        }
        if ("lower".equals(lowerOrUpper)) {
            return new Text(value.toString().toLowerCase());
        } else if ("upper".equals(lowerOrUpper)) {
            return new Text(value.toString().toUpperCase());
        }
        // 转换参数错误的情况下，直接返回原本的值
        return value;
    }

}
