package com.hfy.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 *
 * Created by cage on 2017/10/17.
 */
@Controller
@RequestMapping("user")
public class UserController {

    @GetMapping("getAll")
    @ResponseBody
    public String getUsers(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        if (session != null && session.getAttribute("currentPerson") != null) {
            session.setMaxInactiveInterval(10); // 超时时间10秒
            return request.getSession().getAttribute("currentPerson").toString() + "------zookeeper------";
        } else {
            return "login" + "------zookeeper------";
        }
    }


    @GetMapping("login")
    @ResponseBody
    public void login(HttpServletRequest request) {
        System.out.println("login");
        request.getSession().setAttribute("currentPerson", "zookeeper");
    }
}
