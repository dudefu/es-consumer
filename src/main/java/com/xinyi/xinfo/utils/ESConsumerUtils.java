package  com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class ESConsumerUtils {

    public static final Logger logger = LoggerFactory.getLogger(ESConsumerUtils.class);

    public static void main(String[] args) {
//        getEsDatas("127.0.0.1", "9200", "demo");
//        getEsDatas("10.200.152.77", "29200", "behavior");
        getEsDatas("http://ns.xydev.cn/api", "29200", "person");

        //System.out.println(Constant.channelCount);
    }

    public static List<JSONObject> getEsDatas(String server, String port, String indexName) {

        //修改配置文件
        configProperties(server, port);

        logger.info("开始获取ES数据...");
        List<JSONObject> jsonObjects = new ArrayList<>();
        //创建es客户端工具
        ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil();

        //判读索引是否存在，存在返回true，不存在返回false
        boolean exist = clientUtil.existIndice(indexName);
        if (exist) {
            logger.info("ES索引index ==> " + exist);
            //返回一条ES数据
//            Map dataMap  = clientUtil.searchObject(indexName+"/_search",Map.class);
//            List<Map> listData = new ArrayList<>();
//            listData.add(dataMap);

            //返回所有数据，速度比searchAll快
            //ESDatas<Map> esDatas  = clientUtil.searchList(indexName+"/_search",Map.class);

            ESDatas<Map> esDatas = clientUtil.searchAll(indexName, 10000, Map.class);
            System.out.println("===>>>>"+esDatas.getScrollId());
            long totalCount = esDatas.getTotalSize();
            logger.info(indexName + "查询数据数 ==>> " + totalCount);
            List<Map> listData = esDatas.getDatas();
            for (Map map : listData) {
                String string = JSON.toJSONString(map);
                //System.out.println(string);
                JSONObject jsonObject = JSON.parseObject(string);
                //System.out.println(jsonObject.toJSONString());
                jsonObjects.add(jsonObject);
            }
            long count = clientUtil.countAll(indexName);
            logger.info(indexName + "数据总记录数 ==>> " + count);


        } else {
            logger.warn("ES索引不存在！");
        }
        logger.info("获取ES数据完成！");
        return jsonObjects;
    }

    public static void configProperties(String server, String port) {
        Map properties = new HashMap();
        String host = server + ":" + port;
        //es服务器地址和端口，多个用逗号分隔
        properties.put("elasticsearch.rest.hostNames", host);
        //是否在控制台打印dsl语句，log4j组件日志级别为INFO或者DEBUG
        properties.put("elasticsearch.showTemplate", "true");
        //集群节点自动发现
        properties.put("elasticsearch.discoverHost", "true");
        properties.put("http.timeoutSocket", "60000");
        properties.put("http.timeoutConnection", "40000");
        properties.put("http.connectionRequestTimeout", "70000");
        ElasticSearchBoot.boot(properties);
    }
}
