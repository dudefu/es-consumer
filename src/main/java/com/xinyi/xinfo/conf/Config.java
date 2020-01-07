package com.xinyi.xinfo.conf;

import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;

import java.util.HashMap;
import java.util.Map;

public class Config {

    /**
     * 这里只设置必须的配置项，其他的属性参考配置文件：resources/application.properties
     *
     */
    public static void conf(String server,String port){
        Map properties = new HashMap();
        String hostName = server+":"+port;
        //认证账号和口令配置，如果启用了安全认证才需要，支持xpack和searchguard
        properties.put("elasticUser","elastic");
        properties.put("elasticPassword","changeme");
        //es服务器地址和端口，多个用逗号分隔
        properties.put("elasticsearch.rest.hostNames",hostName);
        //是否在控制台打印dsl语句，log4j组件日志级别为INFO或者DEBUG
        properties.put("elasticsearch.showTemplate","true");
        //集群节点自动发现
        properties.put("elasticsearch.discoverHost","true");
        properties.put("http.timeoutSocket","60000");
        properties.put("http.timeoutConnection","40000");
        properties.put("http.connectionRequestTimeout","70000");
        ElasticSearchBoot.boot(properties);
    }
}
