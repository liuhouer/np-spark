package cn.northpark.javaSpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 需求：使用集合创建RDD
 * Created by xuwei
 */
public class CreateRddByArrayJava {
    public static void main(String[] args) {
        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("CreateRddByArrayJava")
            .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建集合
        List<Integer> arr = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(arr);
        Integer sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        System.out.println(sum);

        sc.stop();
    }
}
