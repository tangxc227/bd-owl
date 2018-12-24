package com.owl.trans.common;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:35 2018/12/24
 * @Modified by:
 */
public interface EventLogConstants {

    /**
     * hbase表名称
     */
    String HBASE_NAME_EVENT_LOGS = "event_logs";

    /**
     * event_logs表的列簇名称
     */
    String EVENT_LOGS_FAMILY_NAME = "info";

    /**
     * 日志分隔符
     */
    String LOG_SEPARATOR = "\\^A";

    /**
     * ip地址
     */
    String LOG_COLUMN_NAME_IP = "ip";

    /**
     * 服务器时间
     */
    String LOG_COLUMN_NAME_SERVER_TIME = "s_time";

    /**
     * 事件名称
     */
    String LOG_COLUMN_NAME_EVENT_NAME = "en";

    /**
     * 数据收集端的版本信息
     */
    String LOG_COLUMN_NAME_VERSION = "ver";

    /**
     * 用户唯一标识符
     */
    String LOG_COLUMN_NAME_UUID = "u_ud";

    /**
     * 会员唯一标识
     */
    String LOG_COLUMN_NAME_MEMBER_ID = "u_mid";

    /**
     * 会话ID
     */
    String LOG_COLUMN_NAME_SESSION_ID = "u_sd";

    /**
     * 客户端时间
     */
    String LOG_COLUMN_NAME_CLIENT_TIME = "c_time";

    /**
     * 语言
     */
    String LOG_COLUMN_NAME_LANGUAGE = "l";

    /**
     * 浏览器user agent参数
     */
    String LOG_COLUMN_NAME_USER_AGENT = "b_iev";

    /**
     * 浏览器分辨率大小
     */
    String LOG_COLUMN_NAME_RESOLUTION = "b_rst";

    /**
     * 当前URL
     */
    String LOG_COLUMN_NAME_CURRENT_URL = "p_url";

    /**
     * 前一个页面的URL
     */
    String LOG_COLUMN_NAME_REFERRER_URL = "p_ref";

    /**
     * 当前页面的title
     */
    String LOG_COLUMN_NAME_TITLE = "tt";

    /**
     * 订单ID
     */
    String LOG_COLUMN_NAME_ORDER_ID = "oid";

    /**
     * 订单名称
     */
    String LOG_COLUMN_NAME_ORDER_NAME = "on";

    /**
     * 订单金额
     */
    String LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT = "cua";

    /**
     * 订单货币类型
     */
    String LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE = "cut";

    /**
     * 订单支付金额
     */
    String LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE = "pt";

    /**
     * category名称
     */
    String LOG_COLUMN_NAME_EVENT_CATEGORY = "ca";

    /**
     * action名称
     */
    String LOG_COLUMN_NAME_EVENT_ACTION = "ac";

    /**
     * kv前缀
     */
    String LOG_COLUMN_NAME_EVENT_KV_START = "kv_";

    /**
     * duration持续时间
     */
    String LOG_COLUMN_NAME_EVENT_DURATION = "du";

    /**
     * 操作系统名称
     */
    String LOG_COLUMN_NAME_OS_NAME = "os";

    /**
     * 操作系统版本
     */
    String LOG_COLUMN_NAME_OS_VERSION = "os_v";

    /**
     * 浏览器名称
     */
    String LOG_COLUMN_NAME_BROWSER_NAME = "browser";

    /**
     * 浏览器版本
     */
    String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_v";

    /**
     * ip地址解析的所属国家
     */
    String LOG_COLUMN_NAME_COUNTRY = "country";

    /**
     * ip地址解析的所属省份
     */
    String LOG_COLUMN_NAME_PROVINCE = "province";

    /**
     * ip地址解析的所属城市
     */
    String LOG_COLUMN_NAME_CITY = "city";

}
