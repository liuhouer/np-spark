package cn.northpark.javaSpark.core.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Action_take {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
        List<String> resultList = lines.take(3);

        for(String one:resultList){
            System.out.println(one);
        }


    }
}
