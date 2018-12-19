package com.owl.javasdk.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:54 2018/12/19
 * @Modified by:
 */
public class HttpRequestUtil {

    private static final Logger LOGGER = Logger.getGlobal();

    /**
     * HTTP GET请求
     *
     * @param url
     * @return
     */
    public static String sendGet(String url) {
        HttpURLConnection connection = null;
        BufferedReader reader = null;
        String result = "";
        try {
            URL obj = new URL(url);
            connection = (HttpURLConnection) obj.openConnection();
            connection.setConnectTimeout(10000);
            connection.setReadTimeout(10000);
            connection.setRequestMethod("GET");
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                result += line;
            }
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "http请求异常：" + url, e);
            throw new RuntimeException("http请求异常：" + url);
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // nothing
                }
            }
            if (null != connection) {
                connection.disconnect();
            }
        }
        return result;
    }

}
