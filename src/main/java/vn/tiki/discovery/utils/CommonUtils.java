package vn.tiki.discovery.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtils
{
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public static String getStringByDate(Date date){
        return sdf.format(date);
    }

    public static Date getDateByString(String string){
        try {
            return sdf.parse(string);
        } catch (ParseException e) {
            e.printStackTrace();
            return new Date();
        }
    }

}
