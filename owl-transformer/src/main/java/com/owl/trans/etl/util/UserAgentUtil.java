package com.owl.trans.etl.util;

import com.owl.trans.etl.model.UserAgentInfo;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;

/**
 * @Author: tangxc
 * @Description: 解析浏览器的user agent的工具类，内部就是调用这个uasparser jar文件
 * @Date: Created in 09:57 2018/12/24
 * @Modified by:
 */
public class UserAgentUtil {

    private static UASparser uaSparser = null;

    static {
        // 初始化uaSparser对象
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (Exception e) {

        }
    }

    /**
     * 解析浏览器的user agent字符串，返回UserAgentInfo对象。<br/>
     * 如果user agent为空，返回null。如果解析失败，也直接返回null。
     *
     * @param userAgent 要解析的user agent字符串
     * @return 返回具体的值
     */
    public static UserAgentInfo analyticUserAgent(String userAgent) {
        UserAgentInfo result = null;
        if (!StringUtils.isBlank(userAgent)) {
            try {
                cz.mallat.uasparser.UserAgentInfo userAgentInfo = uaSparser.parse(userAgent);
                result = new UserAgentInfo();
                result.setBrowserName(userAgentInfo.getUaFamily());
                result.setBrowserVersion(userAgentInfo.getBrowserVersionInfo());
                result.setOsName(userAgentInfo.getOsFamily());
                result.setOsVersion(userAgentInfo.getOsName());
            } catch (Exception e) {
                // 出现异常，将返回值设置为null
                result = null;
            }
        }
        return result;
    }

}
