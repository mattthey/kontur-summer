package ru.gnkoshelev.kontur.intern.redis.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

class Test
{
    private static boolean isCacheables;

    public static boolean isCacheables()
    {
        return isCacheables;
    }
    public static boolean setCacheables(boolean newValueParamIsCacheables)
    {
        return isCacheables = newValueParamIsCacheables;
    }
}

public class Main
{
    public static void main(String[] args) throws InterruptedException
    {
//        testHasMap();
        testRedisMap();
//        jedisTest();
    }

    public static void jedisTest()
    {
        new Thread(() -> {
            Jedis jedis1 = getJedis();
            jedis1.subscribe(new JedisPubSub()
            {
                @Override
                public void onMessage(String channel, String message)
                {
                    System.out.println(String.format("Chanel=%s ; message=%s", channel, message));
                }
            }, "C1");
        }).start();

        new Thread(() -> {
            Jedis jedis2 = getJedis();
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");
            jedis2.publish("C1", "asd");

        }).start();

    }

    public static void testHasMap()
    {
        HashMap<String, String> map = new HashMap<>();
        map.putAll(Map.of("1", "2", "3", "4", "5", "6"));
        Set<Entry<String, String>> entrySet = map.entrySet();
        map.put("7", "8");

        // для добавления одного элемента
        Entry<String, String> forAdd = entrySet.iterator().next();
//        forAdd.setValue("123");
//        entrySet.add(forAdd);

        // удаляет элемент успешно.
//        Entry<String, String> forRemove = entrySet.iterator().next();
//        entrySet.remove(forRemove);

        // функция clear работает
//        entrySet.clear();

        printlnMap(map);
//        printCollections(entrySet);
    }

    public static void testRedisMap()
    {
        RedisMap redisMap = new RedisMap();
        RedisMap redisMap2 = new RedisMap(redisMap.getUuid());
//        redisMap.putAll(Map.of("1", "2", "3", "4", "5", "6"));
//        Set<Entry<String, String>> entrySet = redisMap.entrySet();

//        Entry<String, String> forDelete = entrySet.iterator().next();
//        entrySet.remove(forDelete);

//        entrySet.remove(forRemove);

        printAllData();
    }

    public static void printAllData()
    {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys("*");
        System.out.println(String.format("keys size = %d", keys.size()));
        for (String k : keys) {
            println(String.format("for keys=%s", k));
            printlnMap(jedis.hgetAll(k));
        }
        jedis.close();
    }

    public static Jedis getJedis()
    {
        Jedis jedis = new Jedis("redis-14342.c11.us-east-1-3.ec2.cloud.redislabs.com", 14342);
        jedis.auth("ZxiKr5RpEzXX8ZRLAX5hUu6E88UNbUk5");
        return jedis;
    }

    public static void println(Object o)
    {
        System.out.println(o);
    }

    public static <A> void printCollections(Collection<A> collection)
    {
        for (A item : collection)
            System.out.println(item.toString());
    }

    public static <K, V> void printlnMap(Map<K, V> map)
    {
        for (Map.Entry<K, V> m : map.entrySet())
            System.out.println(String.format("\tkey=%s ; value=%s", m.getKey(), m.getValue()));
    }
}