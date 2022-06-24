package cn.northpark.javaSpark.core.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairWords = words.mapToPair( word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> reduceResult = pairWords.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<Integer, String> transRDD = reduceResult.mapToPair(tp -> new Tuple2<>(tp._2, tp._1));
        JavaPairRDD<String, Integer> result = transRDD.sortByKey(false).mapToPair(tp -> new Tuple2<>(tp._2, tp._1));
        /**
         * System.out::println 这种方式会报序列化问题，不要使用
         */
//        result.foreach(System.out::println);
        result.collect().forEach(System.out::println);



//        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
//        JavaPairRDD<String, Integer> pairWords = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
//        JavaPairRDD<String, Integer> reduceResult = pairWords.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
//        JavaPairRDD<Integer, String> transRDD = reduceResult
//                    .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) tp -> new Tuple2<>(tp._2, tp._1));
//        JavaPairRDD<String, Integer> result = transRDD.sortByKey(false).mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) tp -> new Tuple2<>(tp._2, tp._1));
//        result.foreach((VoidFunction<Tuple2<String,Integer>>) tp-> System.out.println(tp));
        sc.stop();
    }
}
