package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.*;

import java.io.FileInputStream;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * @author Gregory Koshelev
 * @updated Matvey Noskov
 */
public class RedisMap implements Map<String,String>
{
    private static Jedis jedis;

    static {
        try (FileInputStream fileInputStream = new FileInputStream("./application.properties")) {
            Properties properties = new Properties();
            properties.load(fileInputStream);
            jedis = new Jedis(properties.getProperty("db.redis.host"), Integer.parseInt(properties.getProperty("db.redis.port")));
            jedis.auth(properties.getProperty("db.redis.password"));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to connect to the redis database. Check application.properties " + ex.getMessage());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            jedis.flushDB();
            System.out.println("Database clear");
            jedis.close();
            System.out.println("Database connection close");
        }));
    }

    private final String id;

    public RedisMap() {
        id = "id:" + this.toString() + "-";
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
        return jedis.exists(id + key);
    }

    @Override
    public boolean containsValue(final Object value) {
        // TODO доделать
        Set<String> keys = keySet();
        for (String k : keys)
        {
            String actualValue = get(k);
            if (actualValue != null && actualValue.equals(value))
                return true;
        }
        return false;
    }

    @Override
    public String get(final Object key) {
         return jedis.get(id + key.toString());
    }

    @Override
    public String put(final String key, final String value) {
        // TODO доделать
        return jedis.getSet(id + key, value);
    }

    @Override
    public String remove(final Object key) {
        Transaction tr = jedis.multi();
        tr.get(id + key);
        tr.del(id + key);
        return (String) tr.exec().get(0);
    }

    public void remove(String... keys) {
        // TODO доделать
        String[] actualKey = Arrays.stream(keys).map(k -> id + k).toArray(String[]::new);
        jedis.del(actualKey);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends String> m) {
        // TODO доделать
//        Transaction tr = jedis.multi();
//        m.forEach((k, v) -> tr.set(k, v));
//        tr.exec();
        String[] keyValey = m.keySet().stream().collect(Collector.of(
                (Supplier<LinkedList<String>>) LinkedList::new,
                (l, i) -> { l.add(id + i); l.add(m.get(i)); },
                (l1, l2) -> { l1.addAll(l2); return l1; },
                (l) -> l.toArray(String[]::new)
        ));
        jedis.msetnx(keyValey);
    }

    @Override
    public void clear() {
        // TODO доделать
        Set<String> keys = keySet();
        String[] keysForRemove = keys.toArray(new String[keys.size()]);
        if (keysForRemove.length != 0)
            remove(keysForRemove);
    }

    @Override
    public Set<String> keySet() {
        // TODO доделать
         Set<String> keys = jedis.keys(id + "*");
         return keys.stream().map(k -> k.substring(id.length())).collect(Collectors.toSet());
    }

    @Override
    public Collection<String> values() {
        // TODO доделать
        Collection<String> result = new LinkedList<>();
        Set<String> keys = keySet();
        for (String key : keys)
            result.add(get(key));
        return result;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        Map<String, String> result = new HashMap<>();
        Set<String> keys = keySet();
        for (String k : keys)
            result.put(k, get(k));
        return result.entrySet();
    }

    private String getKeyInDatabase(String userKey) {
        return id + userKey;
    }

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
