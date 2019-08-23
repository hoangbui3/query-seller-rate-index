package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesReader {

    private static Properties instance;

    public static synchronized Properties getProp() {

        if (instance == null) {
            instance = loadProperties();
        }

        return instance;

    }

    private static synchronized Properties loadProperties() {
        Properties prop = new Properties();

        try {
            FileInputStream ip = new FileInputStream("config.properties");
            prop.load(ip);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return prop;
    }
}
