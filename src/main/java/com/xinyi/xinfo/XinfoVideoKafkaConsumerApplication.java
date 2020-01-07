package com.xinyi.xinfo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.xinyi.xinfo.runner.StartupRunner;

@SpringBootApplication
public class XinfoVideoKafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(XinfoVideoKafkaConsumerApplication.class, args);
    }

    @Bean
    public StartupRunner startupRunner(){
        return new StartupRunner();
    }

}
