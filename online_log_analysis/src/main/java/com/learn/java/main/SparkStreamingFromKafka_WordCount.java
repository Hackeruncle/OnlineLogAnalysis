package com.learn.java.main;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;

import scala.Tuple2;

import com.google.common.collect.Lists;


/**wordcount案例：
 * Created by jpwu on 2016/12/28.
 *
 * 基于Direct方式：
 *      1.周期性查询kafka，来获得每个topic+partition的最新的offset，从而定义每个batch的offset的范围。
 *      2.当处理数据的job启动时，就会使用kafka的简单comsumer api来获取kafka指定offset范围的数据。
 *
 *  该方式的优点：
 *      1.简化并行读取：
 *      如果读取多个partition，就不需要创建多个输入DStream，然后对它们进行union操作。
 *      Spark会创建跟kafka partition一样多的RDD partition，并且会并行从kafka中读取数据。所以kafka partition 和 RDD partition之间，有一一映射关系。
 *
 *      2.高性能：如何保证数据零丢失？
 *      a.在基于Receiver的方式中，需要开启WAL（Write Ahead Logs）机制，这种方式会实际上会先把数据写到一个持久化的日志中，
 *      然后对数据做操作，如果操作过程中系统挂了，恢复的时候可以重新读取日志文件再次进行操作。而kafka本身就有高可靠的机制，
 *      数据本身在kafka中就会复制多份，所以这种方式会效率低下。
 *
 *      b.在基于Direct的方式中，不依赖Receiver,也不需要开启WAL机制，只要kafka中的数据做了多份复制，就可以通过kafka的副本进行恢复。
 *
 *      3.一次且仅一次的事务机制：
 *      a.在基于Receiver的方式中，是使用kafka的高阶API来在zookeeper中保存消费过的offset的。这是消费kafka数据的传统方式。这种方式配合着WAL机制可以保证数据
 *      零丢失的高可靠性的，但是缺无法保证数据被处理一次且一次，可能会处理2次。因为spark与zookeeper之间可能不同步的。
 *
 *      b.在基于Direct的方式中，使用kafka的简单API，spark streaming自己就负责追踪消费的offset，并保存在checkpoint中。spark自己一定是同步的，因此可以保证
 *      数据是被消费一次且仅一次。
 *
 *   https://github.com/apache/spark/tree/v2.0.2/examples/src/main/java/org/apache/spark/examples/streaming
 *
 */
public class SparkStreamingFromKafka_WordCount {
    //声明一个以空格为正则的Pattern的regexSpace对象
    static final Pattern regexSpace = Pattern.compile(" ");

    public static void main(String[] args) {
        //每个话题的分片数
        int numThreads=2;
        //使用SparkConf 来创建JavaStreamingContext,其每隔1秒作为输入流
        SparkConf sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
        JavaStreamingContext jssc= new JavaStreamingContext(sparkConf,new Duration(1000));// 1秒

        //开启checkpoint机制，把checkpoint中的数据目录设置为hdfs目录
        /*
        hdfs dfs -mkdir -p hdfs://172.16.101.56:8020/spark/checkpointdata
        hdfs dfs -chmod -R 777 hdfs://172.16.101.56:8020/spark/checkpointdata
        hdfs dfs -ls hdfs://172.16.101.56:8020/spark/checkpointdata
         */
        jssc.checkpoint("hdfs://172.16.101.56:8020/spark/checkpointdata");


        //设置kafka的map参数
        Map<String,Object> kafkaParams = new HashMap<String,Object>();
        kafkaParams.put("bootstrap.servers","172.16.101.58:9092,172.16.101.59:9092,172.16.101.60:9092"); //定义kakfa 服务的地址
        kafkaParams.put("key.deserializer",StringDeserializer.class);//key的序列化类
        kafkaParams.put("value.deserializer",StringDeserializer.class);//value的序列化类
        kafkaParams.put("group.id","use_a_separate_group_id_for_each_stream");//制定consumer group
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("enable.auto.commit",false);//是否自动确认offset

        //创建要从kafka去读取的topic集和对象，两个topic
        //Collection<String> topics =Arrays.asList("test3","test4");
        Collection<String> topics =Arrays.asList("logtopic");

        //输入流:
        // LocationStrategies.PreferConsistent()   本地策略.一致化
        // ConsumerStrategies.<String,String> Subscribe(topics,kafkaParams)  消费者订阅的topic, 可同时订阅多个,传入参数为topic和kafka参数
        final JavaInputDStream<ConsumerRecord<String,String>> lines = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String,String> Subscribe(topics,kafkaParams));

        //将 以空格拆分 拆分成单词
        //transformation操作：flatMap
        JavaDStream<String> words= lines.flatMap(
                new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                public Iterator<String> call(ConsumerRecord<String, String> t) throws Exception {
                    return Lists.newArrayList(regexSpace.split(t.value())).iterator();
                }
        });

        //返回每个单词，计数为1 的元组，转换为pair rdd
        //transformation操作：mapToPair
        JavaPairDStream<String,Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s,1);
                    }
                 }
        );


        //按照key，累计value（相当于select key,count(value) sumvalue from t group by key;）
        //实验1：transformation操作，reduceByKey函数，统计每一个batch的每个单词的所出现的次数
        JavaPairDStream<String,Integer> wordcount= pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
        });


        /*
        //实验2：updateStateByKey函数，统计每个单词的历史上所出现次数
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                        Integer newSum =state.or(0);
                        for(Integer i: values){
                            newSum +=i;
                        }
                        return Optional.of(newSum);
                    }
                };
        JavaPairDStream<String, Integer> wordcount = pairs.updateStateByKey(updateFunction);
        */

        /*
         //实验3：窗口操作，reduceByKeyAndWindow函数，统计每隔1秒计算一下最近5秒的每个单词总数
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
           public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };
        JavaPairDStream<String, Integer> wordcount = pairs.reduceByKeyAndWindow(reduceFunc, Durations.seconds(5), Durations.seconds(1));
        */
        //output操作: 触发job执行
        wordcount.print();

        try {

            jssc.start(); //启动
            jssc.awaitTermination(); //等待中断
            jssc.close(); //关闭


        }catch (InterruptedException e){

            e.printStackTrace();

        }




    }



}
