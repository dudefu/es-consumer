package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableOperate {

    private static final Logger logger = LoggerFactory.getLogger(TableOperate.class);
    private static final Connection conn =  GreenPlumUtils.getConnection();

    public static List<String> getFields(String tableName){

        String columnSql = "select COLUMN_NAME from  information_schema.COLUMNS where TABLE_NAME = \'"+tableName+"\'";
        List<String> fields = new ArrayList<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(columnSql);
            while (rs.next()) {
                fields.add(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fields ;
    }

    public static boolean addFields(String tableName, List<String> fields){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(" ADD "+fields.get(i).replace("@","_")+" varchar(255),");
        }
        String columns = sb.toString().substring(0, sb.toString().lastIndexOf(",")) ;
        String columnSql = "ALTER TABLE "+tableName+columns;
        logger.info(columns);
        logger.info(columnSql);
        try {
            Statement st = conn.createStatement();
            boolean rs = st.execute(columnSql);
            return rs ;
        } catch (Exception e) {
            e.printStackTrace();
            return false ;
        }
    }

    public static boolean updateFields(String tableName,String field,String type,int len){
        boolean bool = true ;
        String columnSql = null ;
        if("character".equals(type)) {
            columnSql = "ALTER TABLE " + tableName + " alter COLUMN " + field.replace("@", "_") + " type character varying(" + len + ")";
        }
        if("text".equals(type)){
            columnSql = "ALTER TABLE " + tableName + " alter COLUMN " + field.replace("@", "_") + " type text";
        }
        logger.info(columnSql);
        try {
            Statement st = conn.createStatement();
            bool = st.execute(columnSql);
        } catch (Exception e) {
            e.printStackTrace();
            bool = false ;
        }
        return bool ;
    }

    public static String getFields(JSONObject jsonObject, String tableName){
        StringBuilder result = new StringBuilder();
        int count = 0 ;
        List<String> addFields = new ArrayList<>();

        //获取表字段
        List<String> fields = TableOperate.getFields(tableName);

        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            count++;
            String key = entry.getKey().replace("@","_").toLowerCase() ;
            if(count != jsonObject.entrySet().size()) {
                if (key.equals("id")) {
                    result.append("_id").append(",");
                } else if (key.equals("data")) {
                    result.append("_data").append(",");
                } else if (key.equals("user")) {
                    result.append("_user").append(",");
                } else {
                    result.append(key).append(",");
                }
            }else{
                if (key.equals("id")) {
                    result.append("_id");
                } else if (key.equals("data")) {
                    result.append("_data");
                } else if (key.equals("user")) {
                    result.append("_user");
                } else {
                    result.append(key);
                }
            }

            //判断数据里的字段是否包含在表中字段里，不包含的话，往表中新增字段
            boolean bool =  fields.contains(key);
            if(!bool){
                addFields.add(key);
            }
        }
        if(addFields.size() != 0 ){
            TableOperate.addFields(tableName,addFields);

        }
        return result.toString() ;
    }

    public static Map<String, Integer> getFieldsLength(String tableName, Connection conn) {
        Map<String, Integer> result = new HashMap<>();
        String sql = "select column_name,character_maximum_length from information_schema.columns where table_name= '" + tableName + "'";
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                result.put(rs.getString(1), rs.getInt(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Map<String, String> getFieldsType(String tableName, Connection conn) {
        Map<String, String> result = new HashMap<>();
        String sql = "select column_name,udt_name from information_schema.columns where table_name= '" + tableName + "'";
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                result.put(rs.getString(1), rs.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static boolean addFields(String tableName, List<String> fields, Connection conn) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(" ADD " + fields.get(i).replace("@","_") + " varchar(255),");
        }
        String columns = sb.toString().substring(0, sb.toString().lastIndexOf(","));
        String columnSql = "ALTER TABLE " + tableName + columns;
        logger.info(columns);
        logger.info(columnSql);
        try {
            Statement st = conn.createStatement();
            boolean rs = st.execute(columnSql);
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static List<String> getFields(String tableName, Connection conn) {
        String columnSql = "select COLUMN_NAME from  information_schema.COLUMNS where TABLE_NAME = \'" + tableName + "\'";
        List<String> fields = new ArrayList<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(columnSql);
            while (rs.next()) {
                fields.add(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fields;
    }

    public static int getColumnNum(String tableName, Connection conn) {

        String fieldNumSql = "select count(*) from  information_schema.COLUMNS where TABLE_NAME = \'" + tableName + "\'";
        int num = 0;
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(fieldNumSql);
            while (rs.next()) {
                num = rs.getInt(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return num;
    }
}
