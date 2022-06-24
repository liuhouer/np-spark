package cn.northpark.javaSpark.core.examples;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopN {
    public static void main(String[] args) {
        SparkConf conf =new SparkConf();
        conf.setMaster("local");
        conf.setAppName("topn");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/scores.txt");
        JavaPairRDD<String, Integer> pairInfo = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<String, Integer>(line.split("\t")[0], Integer.valueOf(line.split("\t")[1]));
            }
        });

        pairInfo.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tp) throws Exception {
                String className = tp._1;
                List<Integer> scoreList = IteratorUtils.toList(tp._2.iterator());
                Collections.sort(scoreList, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2-o1;
                    }
                });

                if(scoreList.size()>3){
                    System.out.println("className = "+className);
                    for(int i =0 ;i<3;i++){
                        System.out.println("Socre = "+scoreList.get(i));
                    }
                }else{
                    System.out.println("className = "+className);
                    for(int i =0 ;i<scoreList.size();i++){
                        System.out.println("Socre = "+scoreList.get(i));
                    }
                }

            }
        });

    }
}
