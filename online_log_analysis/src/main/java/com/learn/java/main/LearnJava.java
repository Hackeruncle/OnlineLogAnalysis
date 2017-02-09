package com.learn.java.main;


import com.learn.scala.main.ScalaPrint;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.Timestamp;


import org.influxdb.*;
/**
 * Created by jpwu on 2016/12/27.
 */
public class LearnJava {
    public static void main(String[] args) {
        //调用scala
        //ScalaPrint.print();

        /* 1.List的使用

        Collection<String> topics =Arrays.asList("test3","test4");

        //topics.add("test5"); //添加  --error

        System.out.println(topics.size());  //元素数
        System.out.println(topics.toString()); //打印

        Object[] object  = topics.toArray();
        System.out.println(object[0]);//取第一个元素

        topics.remove("test4");//移除test4对象
        System.out.println(topics.toString()); //打印
        topics.clear();//清除容器的所有对象
        System.out.println(topics.toString()); //打印
     */

        //Tuple2 元组
        //Tuple2 tuple2 = new  Tuple2<String, Integer>("abc",1);
        //System.out.println( tuple2._1().toString());
        //System.out.println( tuple2._2().toString());
        //System.out.println(tuple2.toString());


        //截取字符串
//        String str1 = "flume-agent-01 namenode 2017-01-13 19:57:16,367 INFO org.apache.hadoop.ipc.Server: Served: monitorHealth queueTime= 0 procesingTime= 0";
//
//        String[] spiltstr=str1.split(" ");
//        System.out.println(spiltstr[0]);
//        System.out.println(spiltstr[1]);
//        System.out.println(spiltstr[2]+" "+spiltstr[3]);
//        System.out.println(spiltstr[4]);
//
//        System.out.println(str1.substring(spiltstr[0].length()+spiltstr[1].length()+spiltstr[2].length()+spiltstr[3].length()+spiltstr[4].length()+spiltstr[4].length()+1));
//
//
//        Timestamp  recordTimestamp=new Timestamp(System.currentTimeMillis());
//        System.out.println(recordTimestamp.toString());
//
//
//        //设置Jedis
//        JedisPool pool = new JedisPool(new JedisPoolConfig(), "172.16.101.66",6379);
//        Jedis jedis = pool.getResource();
//        jedis.set("key","value");

        try{
            InfluxDBTest test = new InfluxDBTest();

            test.testWriteMultipleStringData();
        }catch (Exception e){
            System.out.println(e.toString());
        }




    }


}
