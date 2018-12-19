package com.owl.javasdk.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:55 2018/12/19
 * @Modified by:
 */
public class StringUtils {

    /**
     * 判断字符串是否为空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        if (null == str || "".equals(str.trim())) {
            return true;
        }
        return false;
    }

    /**
     * 判断字符串是否不为空
     *
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 构建url
     *      拼接url和参数
     *
     * @param url
     * @param paramsMap
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String buildUrl(String url, Map<String, String> paramsMap) throws UnsupportedEncodingException {
        StringBuffer buffer = new StringBuffer();
        buffer.append(url).append("?");
        for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
            if (isNotEmpty(entry.getKey()) && isNotEmpty(entry.getValue())) {
                buffer.append(entry.getKey().trim())
                        .append("=")
                        .append(URLEncoder.encode(entry.getValue().trim(), "UTF-8"))
                        .append("&");
            }
        }
        return buffer.substring(0, buffer.length() - 1);
    }

}
