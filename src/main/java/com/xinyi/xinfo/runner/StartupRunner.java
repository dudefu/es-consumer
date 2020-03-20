package com.xinyi.xinfo.runner;

import com.xinyi.xinfo.consumer.ES2DBDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;

@Order(1)
public class StartupRunner implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(StartupRunner.class);

    @Override
    public void run(String... args){
        /**
         * eg: streaminglog,streaminglog_1574991501027,streaminglog_157479797979797
         * args[0] --- server
         * args[1] --- port
         * args[2] --- indexName
         * args[3] --- targetTableName
         */
        String server = args[0] ;
        String port = args[1] ;
        String indexName = args[2] ;
        String targetTableName = args[3] ;

        if(args.length == 0){
            logger.warn("main方法缺少运行参数（server,port,indexName,targetTableName）");
            System.exit(0);
        }else{
            ES2DBDemo es2DBDemo = new ES2DBDemo();

            es2DBDemo.directExport(server,port,indexName,targetTableName);

        }
    }
}
