package com.xinyi.xinfo.utils;

import java.util.HashMap;
import java.util.Map;

public class MapKeyToLower {

    public static void main(String[] args) {

    }

    public static Map<String,Object> getMapKeyToLower(Map<String,Object> map){
        Map mapNew = new HashMap();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            mapNew.put(entry.getKey().toLowerCase(),entry.getValue());
        }
        return mapNew ;
    }
}
