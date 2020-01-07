package com.xinyi.xinfo.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableOperate {

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
}
