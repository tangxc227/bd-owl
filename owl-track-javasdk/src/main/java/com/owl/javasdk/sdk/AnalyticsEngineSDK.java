package com.owl.javasdk.sdk;

import com.owl.javasdk.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:57 2018/12/19
 * @Modified by:
 */
public class AnalyticsEngineSDK {

    private static final Logger LOGGER = Logger.getGlobal();

    private static final String ACCESS_URL = "http:127.0.0.1/aa.gif";
    private static final String PLATFORM_NAME = "java_server";
    private static final String SDK_NAME = "jdk";
    private static final String VERSION = "1.0";

    /**
     * 触发订单支付成功事件，发送事件数据到服务器
     *
     * @param orderId
     * @param memberId
     * @return
     */
    public static boolean onChargeSuccess(String orderId, String memberId) {
        try {
            if (StringUtils.isEmpty(orderId) || StringUtils.isEmpty(memberId)) {
                LOGGER.log(Level.WARNING, "订单ID和会员ID不能为空");
                return false;
            }
            Map<String, String> paramsMap = new HashMap<>();
            paramsMap.put("u_mid", memberId);
            paramsMap.put("oid", orderId);
            paramsMap.put("c_time", String.valueOf(System.currentTimeMillis()));
            paramsMap.put("version", VERSION);
            paramsMap.put("en", "e_cs");
            paramsMap.put("pl", PLATFORM_NAME);
            paramsMap.put("sdk", SDK_NAME);
            String url = StringUtils.buildUrl(ACCESS_URL, paramsMap);
            SendDataMonitor.addSendUrl(url);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "发送数据异常", e);
        }

        return false;
    }

    /**
     * 触发订单退款事件，发送退款数据到服务器
     *
     * @param orderId
     * @param memberId
     * @return
     */
    public static boolean onChargeRefund(String orderId, String memberId) {
        try {
            if (StringUtils.isEmpty(orderId) || StringUtils.isEmpty(memberId)) {
                LOGGER.log(Level.WARNING, "订单ID和会员ID不能为空");
                return false;
            }
            Map<String, String> paramsMap = new HashMap<>();
            paramsMap.put("u_mid", memberId);
            paramsMap.put("oid", orderId);
            paramsMap.put("c_time", String.valueOf(System.currentTimeMillis()));
            paramsMap.put("version", VERSION);
            paramsMap.put("en", "e_cr");
            paramsMap.put("pl", PLATFORM_NAME);
            paramsMap.put("sdk", SDK_NAME);
            String url = StringUtils.buildUrl(ACCESS_URL, paramsMap);
            SendDataMonitor.addSendUrl(url);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "发送数据异常", e);
        }
        return false;
    }

}
