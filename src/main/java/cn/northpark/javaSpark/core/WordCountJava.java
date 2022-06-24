package cn.northpark.javaSpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 需求：单词计数
 * Created by xuwei
 */
public class WordCountJava {
    public static void main(String[] args) {
        //第一步：创建JavaSparkContext
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava");
                //.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //第二步：加载数据
        String path = "D:\\hello.txt";
        if(args.length==1){
            path = args[0];
        }
        JavaRDD<String> linesRDD = sc.textFile(path);
        //第三步：对数据进行切割，把一行数据切分成一个一个的单词
        //注意：FlatMapFunction的泛型，第一个参数表示输入数据类型，第二个表示是输出数据类型
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //第四步：迭代words，将每个word转换为(word,1)这种形式
        //注意：PairFunction的泛型，第一个参数是输入数据类型
        //第二个是输出tuple中的第一个参数类型，第三个是输出tuple中的第二个参数类型
        //注意：如果后面需要使用到....ByKey，前面都需要使用mapToPair去处理
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //第五步：根据key(其实就是word)进行分组聚合统计
        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        //第六步：将结果打印到控制台
        wordCountRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tup) throws Exception {
                System.out.println(tup._1+"--"+tup._2);
            }
        });
        //第七步：停止SparkContext
        sc.stop();
    }
}
