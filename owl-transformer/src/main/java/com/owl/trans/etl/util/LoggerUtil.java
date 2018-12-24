package com.owl.trans.etl.util;

import com.owl.trans.common.EventLogConstants;
import com.owl.trans.etl.model.UserAgentInfo;
import com.owl.trans.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: tangxc
 * @Description: 时间控制工具类
 * @Date: Created in 11:08 2018/12/24
 * @Modified by:
 */
public class LoggerUtil {

    private static final Logger LOGGER = Logger.getLogger(LoggerUtil.class);

    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    /**
     * 处理日志数据logText，返回处理结果map集合<br/>
     * 如果logText没有指定数据格式，那么直接返回empty的集合
     *
     * @param logText
     * @return
     */
    public static Map<String, String> handleLog(String logText) {
        Map<String, String> clientInfo = new HashMap<>();
        if (StringUtils.isNotBlank(logText)) {
            String[] tokens = logText.trim().split(EventLogConstants.LOG_SEPARATOR);
            if (tokens.length == 4) {
                // 日志格式为: ip^A服务器时间^Ahost^A请求参数
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, tokens[0].trim());
                // 设置服务器时间
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(TimeUtil.parseNginxServerTime2Long(tokens[1].trim())));
                int index = tokens[3].indexOf("?");
                if (index > -1) {
                    // 获取请求参数，也就是我们的收集数据
                    String requestBody = tokens[3].substring(index + 1);
                    // 处理请求参数
                    handleRequestBody(requestBody, clientInfo);
                    // 处理userAgent
                    handleUserAgent(clientInfo);
                    // 处理ip地址
                    handleIp(clientInfo);
                } else {
                    clientInfo.clear();
                }
            }
        }
        return clientInfo;
    }

    /**
     * 处理请求参数
     *
     * @param requestBody
     * @param clientInfo
     */
    private static void handleRequestBody(String requestBody, Map<String, String> clientInfo) {
        if (StringUtils.isNotBlank(requestBody)) {
            String[] requestParams = requestBody.split("&");
            for (String param : requestParams) {
                if (StringUtils.isNotBlank(param)) {
                    int index = param.indexOf("=");
                    if (index < 0) {
                        LOGGER.warn("没法进行解析参数:" + param + "， 请求参数为:" + requestBody);
                        continue;
                    }

                    String key = null, value = null;
                    try {
                        key = param.substring(0, index);
                        value = URLDecoder.decode(param.substring(index + 1), "utf-8");
                    } catch (Exception e) {
                        LOGGER.warn("解码操作出现异常", e);
                        continue;
                    }
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                        clientInfo.put(key, value);
                    }
                }
            }
        }
    }

    /**
     * 处理浏览器的userAgent信息
     *
     * @param clientInfo
     */
    private static void handleUserAgent(Map<String, String> clientInfo) {
        if (clientInfo.containsKey(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT)) {
            String userAgent = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
            UserAgentInfo info = UserAgentUtil.analyticUserAgent(userAgent);
            if (null != info) {
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, info.getOsName());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, info.getOsVersion());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, info.getBrowserName());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, info.getBrowserVersion());
            }
        }
    }

    /**
     * 处理IP地址
     *
     * @param clientInfo
     */
    private static void handleIp(Map<String, String> clientInfo) {
        if (clientInfo.containsKey(EventLogConstants.LOG_COLUMN_NAME_IP)) {
            String ip = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_IP);
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(ip);
            if (null != info) {
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
            }
        }
    }

}
