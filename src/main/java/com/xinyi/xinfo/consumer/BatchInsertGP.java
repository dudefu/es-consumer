package com.xinyi.xinfo.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.xinyi.xinfo.utils.CopyDataToGp;
import com.xinyi.xinfo.utils.ESConsumerUtils;
import com.xinyi.xinfo.utils.GreenPlumUtils;
import com.xinyi.xinfo.utils.TableOperate;
import org.frameworkset.spi.assemble.PropertiesContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

public class BatchInsertGP {

    public static final Logger logger = LoggerFactory.getLogger(BatchInsertGP.class);
    private static PropertiesContainer propertiesContainer = new PropertiesContainer();
    private static final Connection conn = GreenPlumUtils.getConnection();

    static {
        propertiesContainer.addConfigPropertiesFile("application.properties");
    }

    /**
     * 一次性插入数据
     * @param esDatas
     * @return
     */
    public static boolean batchInsert(List<JSONObject> esDatas,String targetTableName) {

        boolean result = true;
        Map<Integer, JSONObject> map = new HashMap<>();

        //循环遍历每个jsonobject，得到长度的map集合
        for (JSONObject jsonObject : esDatas) {
            map.put(jsonObject.size(), jsonObject);
        }

        try {

            List<Integer> list = new ArrayList<>();
            for (Integer key : map.keySet()) {
                list.add(key);
            }
            //对得到的map集合进行排序
            Collections.sort(list);

            int dataSize = esDatas.size();
            if (dataSize != 0) {
                //获取data字段
                String tableColumns = TableOperate.getFields(map.get(list.get(list.size() - 1)), targetTableName).toLowerCase();
                logger.info("===>>> tableColumns : " + tableColumns);
                logger.info("==> 开始往GP数据库插入json数据");
                long copyDataResult = CopyDataToGp.copyData(targetTableName, tableColumns, esDatas, conn);
                logger.info("==> 已插入数据条数 ：" + copyDataResult);
            } else {
                result = false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }
        return result;
    }
}
