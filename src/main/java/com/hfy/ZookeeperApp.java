package com.hfy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 * Created by cage on 2017/10/17.
 */
@SpringBootApplication
@ComponentScan("com.hfy")
@ServletComponentScan   // filter需要
public class ZookeeperApp {

    public static void main(String[] args) {
        SpringApplication.run(ZookeeperApp.class, args);
    }
}
