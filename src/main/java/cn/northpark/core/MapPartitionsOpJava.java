package cn.northpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 需求：mapPartitons的使用
 * Created by xuwei
 */
public class MapPartitionsOpJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("MapPartitionsOpJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
        Integer sum = dataRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> it) throws Exception {
                //数据库链接的代码需要放在这个位置
                ArrayList<Integer> list = new ArrayList<>();
                while (it.hasNext()) {
                    list.add(it.next() * 2);
                }
                //关闭数据库链接
                return list.iterator();
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        System.out.println("sum:"+sum);

    }
}
