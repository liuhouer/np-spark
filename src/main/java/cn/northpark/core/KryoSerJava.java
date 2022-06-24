package cn.northpark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * 需求：Kryo序列化的使用
 * Created by xuwei
 */
public class KryoSerJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("KryoSerJava")
                .setMaster("local")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.classesToRegister","com.imooc.java.Person");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("hello you", "hello me"));

        JavaRDD<String> wordsRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaRDD<Person> personRDD = wordsRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String word) throws Exception {
                return new Person(word, 18);
            }
        }).persist(StorageLevel.MEMORY_ONLY_SER());

        personRDD.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

        while (true){
            ;
        }

    }
}

class Person implements Serializable{
    private String name;
    private int age;
    Person(String name,int age){
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
