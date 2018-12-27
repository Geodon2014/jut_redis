package com.jut.redis.utils;

import java.util.Arrays;

/***
 * String工具类
 */
public class StringUtil {
    public static String logException(Exception e) {
        String statckTrace = Arrays.toString(e.getStackTrace());
        statckTrace = statckTrace.replace(",", "\r\n");
        StringBuffer sb = new StringBuffer();
        sb.append("----------------------------------------此处发生异常，异常信息为：\r\n").append(e).append("\r\n").append(statckTrace);
        return sb.toString();
    }
}
