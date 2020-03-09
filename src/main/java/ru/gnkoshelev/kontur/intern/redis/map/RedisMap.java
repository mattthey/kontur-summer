package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.io.FileInputStream;
import java.util.*;

/**
 * @author Gregory Koshelev
 * @updated Matvey Noskov
 */
public class RedisMap implements Map<String,String>
{
    private static Jedis jedis;
    private final String uuid;

    static {
        try (FileInputStream fileInputStream = new FileInputStream("./application.properties")) {
            Properties properties = new Properties();
            properties.load(fileInputStream);
            jedis = new Jedis(properties.getProperty("db.redis.host"), Integer.parseInt(properties.getProperty("db.redis.port")));
            jedis.auth(properties.getProperty("db.redis.password"));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to connect to the redis database. Check application.properties " + ex.getMessage());
        }
        System.out.println("asd");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // TODO можно переделать на jul, но это не самая удобная и быстрая система логирования
            jedis.flushDB();
            System.out.println("Database clear");
            jedis.close();
            System.out.println("Database connection close");
        }));
    }

    public RedisMap() {
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        return jedis.hexists(uuid, key.toString());
    }

    @Override
    public boolean containsValue(final Object value) {
        boolean isContainsValue = false;
//        ScanParams scanParams = new ScanParams().count(100);
//        String cursor = "0";
//        do {
//            ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(uuid, cursor, scanParams);
//            isContainsValue = scanResult.getResult().stream().anyMatch(kv -> kv.getValue().equals(value));
//            cursor = scanResult.getCursor();
//        } while (!cursor.equals("0"));
//
//        return isContainsValue;
        return jedis.hvals(uuid).contains(value);
    }

    @Override
    public String get(final Object key) {
        return jedis.hget(uuid, key.toString());
    }

    @Override
    public String put(final String key, final String value) {
        String expectedValue = value == null ? "" : value;
        String prevValue = null;
        Transaction tr = jedis.multi();
        tr.hget(uuid, key);
        tr.hset(uuid, key, expectedValue);
        prevValue = (String) tr.exec().get(0);
        return prevValue;
//        return  (String) tr.exec().get(0);
    }

    @Override
    public String remove(final Object key) {
        Transaction tr = jedis.multi();
        tr.hget(uuid, key.toString());
        tr.hdel(uuid, key.toString());
        return (String) tr.exec().get(0);
    }

    public void remove(String... keys) {
        jedis.hdel(uuid, keys);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends String> m) {
        jedis.hset(uuid, (Map<String, String>) m);
    }

    @Override
    public void clear() {
        // TODO доделать
        jedis.del(uuid);
    }

    @Override
    public Set<String> keySet() {
        return jedis.hkeys(uuid);
    }

    @Override
    public Collection<String> values() {
        return jedis.hvals(uuid);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return jedis.hgetAll(uuid).entrySet();
    }

    // хотелось бы узнать версию джавы, чтобы
    @Override
    protected void finalize() throws Throwable
    {
        try {
            clear();
        } catch (final Throwable ex) {
            throw ex;
        } finally {
            super.finalize();
        }
    }
}
