package cn.northpark.javaSpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 需求：通过文件创建RDD
 * Created by xuwei
 */
public class CreateRddByFileJava {
    public static void main(String[] args) {
        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("CreateRddByArrayJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "D:\\hello.txt";
        path = "hdfs://bigdata01:9000/test/hello.txt";
        JavaRDD<String> rdd = sc.textFile(path, 2);
        //获取每一行数据的长度
        JavaRDD<Integer> lengthRDD = rdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });

        //计算文件内数据的总长度
        Integer length = lengthRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        System.out.println(length);

        sc.stop();
    }
}
