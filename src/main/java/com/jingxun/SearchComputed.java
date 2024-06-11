package com.jingxun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SearchComputed {


    public static void main1(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        try (var fs = FileSystem.get(configuration)) {
            Path path = new Path("file:///Users/keray/Downloads/part-00000-7591fc3c-243c-4211-bb86-32950f82e818-c000.gz.parquet");
        }
        System.out.println("xxxxxxxxxxxxxxxx");
    }

}
