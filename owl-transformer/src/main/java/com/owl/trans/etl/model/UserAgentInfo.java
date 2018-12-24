package com.owl.trans.etl.model;

/**
 * @Author: tangxc
 * @Description: 解析后的浏览器信息model对象
 * @Date: Created in 10:04 2018/12/24
 * @Modified by:
 */
public class UserAgentInfo {

    /**
     * 浏览器名称
     */
    private String browserName;
    /**
     * 浏览器版本号
     */
    private String browserVersion;
    /**
     * 操作系统名称
     */
    private String osName;
    /**
     * 操作系统版本号
     */
    private String osVersion;

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    @Override
    public String toString() {
        return "UserAgentInfo{" +
                "browserName='" + browserName + '\'' +
                ", browserVersion='" + browserVersion + '\'' +
                ", osName='" + osName + '\'' +
                ", osVersion='" + osVersion + '\'' +
                '}';
    }

}
