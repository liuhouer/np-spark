package cn.northpark.core.examples;

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
import java.util.Iterator;
import java.util.List;

public class TopN2 {
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
                Iterator<Integer> iter = tp._2.iterator();

                Integer[] top3Score = new Integer[3];
                while(iter.hasNext()){
                    Integer currScore = iter.next();
                    for(int i =0;i<3;i++){
                        if(top3Score[i]==null){
                            top3Score[i] = currScore;
                            break;
                        }else if(currScore>top3Score[i]){
                            for(int j = 2;j>i;j--){
                                top3Score[j]=top3Score[j-1];
                            }
                            top3Score[i] = currScore;
                            break;
                        }
                    }
                }

                System.out.println("className = "+className);
                for (Integer score :top3Score){
                    System.out.println("score = "+score);
                }

            }
        });

    }
}
