package cn.northpark.javaSpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 需求：设置并行度
 * Created by xuwei
 */
public class MoreParallelismJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("MoreParallelismJava");
        //设置全局并行度
        conf.set("spark.default.parallelism","5");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("hello", "you", "hello", "me", "hehe", "hello", "you", "hello", "me", "hehe"));
        dataRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tup) throws Exception {
                System.out.println(tup);
            }
        });

        sc.stop();
    }

}
