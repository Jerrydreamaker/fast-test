package util;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    public static JedisPool createJedisPool(int maxTotal, int maxIdle, String host, int port){

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //指定连接池中最大空闲连接数
        jedisPoolConfig.setMaxIdle(maxIdle);
        //链接池中创建的最大连接数
        jedisPoolConfig.setMaxTotal(maxTotal);
        //设置创建链接的超时时间
        jedisPoolConfig.setMaxWaitMillis(2000);
        //表示连接池在创建链接的时候会先测试一下链接是否可用，这样可以保证连接池中的链接都可用的。
        jedisPoolConfig.setTestOnBorrow(true);
        return new JedisPool(jedisPoolConfig,host,port);
    }
}
