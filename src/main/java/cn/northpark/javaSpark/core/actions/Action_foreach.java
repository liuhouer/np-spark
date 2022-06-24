package cn.northpark.javaSpark.core.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Action_foreach {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("foreach");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
//        lines.foreach(new VoidFunction<String>() {
//            public void call(String s) throws Exception {
//                System.out.println("line is :"+s);
//            }
//        });
        lines.foreach((line)->System.out.println("line is :"+line));

    }
}
