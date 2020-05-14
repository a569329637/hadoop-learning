package com.gsq.hadoop.hbase.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class IdUtils {

    public static Random random = new Random();

    public static String generateId() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = format.format(new Date());
        id = id.replace("-", "").replace(" ", "").replace(":", "");
        id = "student" + id + random.nextInt(99999);
        return id;
    }

}
