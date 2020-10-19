package com.gsq.hive;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author gsq
 * @date 2020/10/19
 */
public class MyLower extends UDF {

    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        return new Text(s.toString().toLowerCase());
    }

}
