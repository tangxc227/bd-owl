package com.owl.trans.common;

/**
 * @Author: tangxc
 * @Description: 事件枚举类。指定事件的名称
 * @Date: Created in 10:35 2018/12/24
 * @Modified by:
 */
public enum EventEnum {

    /**
     * launch事件，表示第一次访问
     */
    LAUNCH(1, "launch event", "e_l"),
    /**
     * 页面浏览事件
     */
    PAGEVIEW(2, "page view event", "e_pv"),
    /**
     * 订单生产事件
     */
    CHARGEREQUEST(3, "charge request event", "e_crt"),
    /**
     * 订单成功支付事件
     */
    CHARGESUCCESS(4, "charge success event", "e_cs"),
    /**
     * 订单退款事件
     */
    CHARGEREFUND(5, "charge refund event", "e_cr"),
    /**
     * 事件
     */
    EVENT(6, "event duration event", "e_e");

    public final int id;
    public final String name;
    public final String alias;

    EventEnum(int id, String name, String alias) {
        this.id = id;
        this.name = name;
        this.alias = alias;
    }

    /**
     * 获取匹配别名的event枚举对象，如果最终还是没有匹配的值，那么直接返回null。
     *
     * @param alias
     * @return
     */
    public static EventEnum valueOfAlias(String alias) {
        for (EventEnum eventEnum : values() ) {
            if (eventEnum.alias.equals(alias)) {
                return eventEnum;
            }
        }
        return null;
    }

}
