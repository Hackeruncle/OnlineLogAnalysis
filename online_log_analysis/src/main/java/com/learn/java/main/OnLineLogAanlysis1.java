package com.learn.java.main;

import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Created by jpwu on 2017/1/13.
 *
 * 主要使用spark streaming来实现:
 * 1.从kafka0.10 cluster读取log
 * 2.计算 cdh 角色日志，其格式: 机器名称 服务名称 时间 日志级别 日志信息
 * a.每隔1秒统计机器，服务的出现的error次数

 2017-01-13 19:57:16,366 DEBUG org.apache.hadoop.security.Groups: Returning cached groups for 'hdfs'
 2017-01-13 19:57:16,366 DEBUG org.apache.hadoop.hdfs.server.namenode.NameNodeResourceChecker: Space available on volume 'null' is 1025976619008
 2017-01-13 19:57:16,367 INFO org.apache.hadoop.ipc.Server: Served: monitorHealth queueTime= 0 procesingTime= 0


 *
 *
 */
public class OnLineLogAanlysis1 {

    //定义滑动间隔为1秒，即为每1秒计算
    private static  final Duration slide_interval= new Duration(5 * 1000);
    private static final Pattern regexSpace = Pattern.compile(" ");
    static String hostname,servicename,linetimestamp,logtype,loginfo;
    static String linetmp;
    static String[] spiltstr;

    public static void main(String[] args) {
        try {
            //1.使用 SparkSession,JavaSparkContext, JavaStreamingContext来定义 对象 jsc
           SparkSession ss=  new SparkSession.Builder().master("local[2]").appName("OnLineLogAanlysis1").getOrCreate();
           JavaSparkContext sc=new JavaSparkContext(ss.sparkContext());
           JavaStreamingContext jssc= new JavaStreamingContext(sc,slide_interval);



            //2.设置kafka的map参数
            Map<String,Object> kafkaParams = new HashMap<String,Object>();
            kafkaParams.put("bootstrap.servers","172.16.101.58:9092,172.16.101.59:9092,172.16.101.60:9092"); //定义kakfa 服务的地址
            kafkaParams.put("key.deserializer",StringDeserializer.class);//key的序列化类
            kafkaParams.put("value.deserializer",StringDeserializer.class);//value的序列化类
            kafkaParams.put("group.id","use_a_separate_group_id_for_each_stream");//制定consumer group
            kafkaParams.put("auto.offset.reset","latest");
            kafkaParams.put("enable.auto.commit",false);//是否自动确认offset

            //3.创建要从kafka去读取的topic的集合对象
            Collection<String> topics = Arrays.asList("logtopic");

            //4.输入流
            JavaInputDStream<ConsumerRecord<String,String>> lines= KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String,String> Subscribe(topics,kafkaParams));
            /**
            *  5.将每一行的 机器名称，服务名称，时间， 日志级别，日志信息 提取出来
            *    拼接 hostname+"_"+servicename+"_"+logtype
            */
            //transformation操作：flatMap
            JavaDStream<String> words= lines.flatMap(
                    new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                           public Iterator<String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {

                               linetmp=consumerRecord.value();//真正的日志行

                               if(linetmp.contains("INFO")==true || linetmp.contains("WARN")==true || linetmp.contains("ERROR")==true || linetmp.contains("DEBUG")==true){
                                   //一个log的输出的第一行

                                   spiltstr=linetmp.split(" "); //按空格分割
                                   hostname=spiltstr[0];
                                   servicename=spiltstr[1];
                                   //linetimestamp=spiltstr[2]+" "+spiltstr[3];
                                   logtype=spiltstr[4];
                                   //loginfo=linetmp.substring(spiltstr[0].length()+spiltstr[1].length()+spiltstr[2].length()+spiltstr[3].length()+spiltstr[4].length()+spiltstr[4].length()+1);
                                   linetmp=hostname+"_"+servicename+"_"+logtype;
                               }else{
                                   linetmp="unFirstRow";//一个log的输出的非第一行
                               }



                              return Lists.newArrayList(linetmp).iterator();
                         }
            });


            /**
             * 6.将这一行过滤掉
             */
            JavaDStream<String> filterwords=   words.filter(new Function<String, Boolean>() {
                 public Boolean call(String s) throws Exception {
                     return s!="unFirstRow"?true:false;
                 }
             });

            /**
             * 7.将每个字符串转换为 pair rdd
             */

            JavaPairDStream<String,Integer> pairs= filterwords.mapToPair(
                     new PairFunction<String, String, Integer>() {
                         public Tuple2<String,Integer> call(String s) throws Exception{

                             return new Tuple2<String, Integer>(s,1);
                         }
             });


            /**
             *
             * 8.计算出现的次数
             *
             */


         JavaPairDStream<String,Integer> wc= pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
         });

            wc.print();

            jssc.start(); //启动流式计算
            jssc.awaitTermination(); //等待中断
            jssc.close(); //关闭

        }catch (Exception e){

            e.printStackTrace();

        }


    }


}
