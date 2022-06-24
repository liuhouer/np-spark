package cn.northpark.streaming;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class StreamingKafkaDataToRedis {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("StreamingToRedis");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        Collection<String> topics = Arrays.asList("topicA");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "c7node1:9092,c7node2:9092,c7node3:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group1108");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        JavaInputDStream<ConsumerRecord<String, String>> dstream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );


        dstream.foreachRDD(
                recordRDD -> recordRDD.foreachPartition(
                    iterRecords -> {
                        String redisHost = "c7node4";
                        int redisPort = 6379;
                        int redisTimeout = 30000;
                        JedisPool jedisPool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout);
                        Jedis jedis = jedisPool.getResource();
                        jedis.select(5);
                        while(iterRecords.hasNext()){
                            ConsumerRecord<String, String> record = iterRecords.next();
                            String value = record.value();
                            System.out.println("有新的数据"+value);
                            String  redisKey = value.split(" ")[0];
                            String  redisValue = value.split(" ")[1];
                            jedis.hset("hello", redisKey,redisValue);
                            System.out.println("插入Redis 成功");
                        }
                        jedisPool.returnResource(jedis);
                    }
                ));

        jsc.start();
        jsc.awaitTermination();
        jsc.start();
    }
}
