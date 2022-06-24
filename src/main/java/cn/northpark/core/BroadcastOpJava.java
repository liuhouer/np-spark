package cn.northpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;

/**
 * 需求：使用广播变量
 * Created by xuwei
 */
public class BroadcastOpJava {

    public static void main(String[] args) {
        //创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("BroadcastOpJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        int varable = 2;
        //1：定义广播变量
        Broadcast<Integer> varableBroadcast = sc.broadcast(varable);

        //2：使用广播变量
        dataRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i1) throws Exception {
                return i1 * varableBroadcast.value();
            }
        }).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i) throws Exception {
                System.out.println(i);
            }
        });

        sc.stop();
    }
}
