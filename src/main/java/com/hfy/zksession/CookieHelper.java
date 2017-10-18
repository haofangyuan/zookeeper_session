package com.hfy.zksession;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class CookieHelper {

    private static final String SESSION_ID_NAME = "D_SESSION_ID";
    private static final String COOKIE_PATH = "/";

    /**
     * 获取sessionId
     * @param request
     * @param response
     * @return
     */
    public static String getSessionId(HttpServletRequest request,
                                HttpServletResponse response) {
        // cookie中查找
        Cookie cookies[] = request.getCookies();
        String sessionId = "";
        if (cookies != null && cookies.length > 0) {
            for (int i = 0; i < cookies.length; i++) {
                Cookie cookie = cookies[i];
                if (cookie.getName().equals(SESSION_ID_NAME)) {
                    sessionId = cookie.getValue();
                }
            }
        }

        // 没有，增加
        if (sessionId == null || sessionId.length() == 0) {
            sessionId = java.util.UUID.randomUUID().toString();
            response.addHeader("Set-Cookie", SESSION_ID_NAME + "=" + sessionId
                    + ";domain=" + request.getServerName() + ";Path="
                    + COOKIE_PATH + ";HTTPOnly");
        }
        return sessionId;
    }
}
