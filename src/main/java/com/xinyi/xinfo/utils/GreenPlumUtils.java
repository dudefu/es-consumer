package com.xinyi.xinfo.utils;
import org.frameworkset.spi.assemble.PropertiesContainer;

import java.sql.Connection;
import java.sql.DriverManager;

public class GreenPlumUtils {

    public static Connection getConnection(){
        try{
            PropertiesContainer propertiesContainer = new PropertiesContainer();
            propertiesContainer.addConfigPropertiesFile("application.properties");
            String GP_CLASS_NAME  = propertiesContainer.getProperty("gp.class.name");
            String GP_URL  = propertiesContainer.getProperty("gp.url");
            String GP_USERNAME  = propertiesContainer.getProperty("gp.username");
            String GP_PASSWORD  = propertiesContainer.getProperty("gp.password");
            Class.forName(GP_CLASS_NAME);
            Connection connection = DriverManager.getConnection(GP_URL, GP_USERNAME, GP_PASSWORD);
            return connection;
        }catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }
}
