package com.learn.java.main;

import java.io.Serializable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * Created by jpwu on 2017/1/16.
 */
public class RedisUtil implements Serializable{
    private static final long serialVersionUID = 264027256914939178L;

    //Redis server
    private static String redisServer = "172.16.101.66";
    //Redis port
    private static int port = 6379;
    //password
    private static String password = "admin";

    private static int MAX_ACTIVE = 1024;

    private static int MAX_IDLE = 200;

    //waittime and timeout time ms
    private static int MAX_WAIT = 10000;

    private static int TIMEOUT = 10000;

    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;

    static {
        try {
            JedisPoolConfig config = new JedisPoolConfig();
           // config.setMaxActive(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
           // config.setMaxWait(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            jedisPool = new JedisPool(redisServer, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * release resource
     * @param jedis
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);

        }
    }

}
