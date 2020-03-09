package ru.gnkoshelev.kontur.intern.redis.map;

import redis.clients.jedis.*;

import java.io.FileInputStream;
import java.lang.ref.Cleaner;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Gregory Koshelev
 * @updated Matvey Noskov
 */
public class RedisMap implements Map<String,String>, Cleaner.Cleanable
{
    private static class MESSAGES
    {
        private static final String ADD_NODE = "ADD_NODE";
        private static final String REMOVE_NODE = "REMOVE_NODE";
        private static final String EDIT_VALUE_BY_KEY = "EDIT_VALUE_BY_KEY";
    }

    private static final String REDIS_MAP_SHARED_POOL = "REDIS_MAP_SHARED_POOL";
    private static final String ERROR_NULL_MESSAGE = "Не поддерживается значение или ключ null";

    private static HashSet<String> uuidsForCurrentHost = new HashSet<>();
    private static final Logger log = Logger.getLogger(RedisMap.class.getSimpleName());
    private static final JedisPool jedisPool;
    private static final Cleaner cleaner = Cleaner.create();

    private final String uuid;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        String host;
        String password;
        int port;

        try (FileInputStream fileInputStream = new FileInputStream("./application.properties"))
        {
            Properties properties = new Properties();
            properties.load(fileInputStream);
            host = properties.getProperty("db.redis.host");
            port = Integer.parseInt(properties.getProperty("db.redis.port"));
            password = properties.getProperty("db.redis.password");

            poolConfig.setMaxTotal(Integer.parseInt(properties.getProperty("db.redis.maxTotal", "32")));
            poolConfig.setMaxIdle(Integer.parseInt(properties.getProperty("db.redis.maxIdle", "8")));
            poolConfig.setMinIdle(Integer.parseInt(properties.getProperty("db.redis.minIdle", "4")));
            poolConfig.setBlockWhenExhausted(Boolean.parseBoolean(properties.getProperty("db.redis.blockWhenExhausted", "true")));
            poolConfig.setMaxWaitMillis(Integer.parseInt(properties.getProperty("db.redis.maxWaitMillis", "-1")));

        }
        catch (Exception ex)
        {
            throw new RuntimeException("Failed to connect to the redis database. Check application.properties " + ex.getMessage());
        }
        jedisPool = new JedisPool(poolConfig, host, port, 10000, password);
        log.info("create jedisPool");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            if (!jedisPool.isClosed() && !uuidsForCurrentHost.isEmpty())
            {
                try (Jedis jedis = jedisPool.getResource())
                {
                    jedis.del(uuidsForCurrentHost.toArray(String[]::new));
                }
                jedisPool.close();
            }
        }));
    }

    public RedisMap()
    {
        this(UUID.randomUUID().toString());
    }

    public RedisMap(String uuid)
    {
        if (uuid.equals(REDIS_MAP_SHARED_POOL))
        {
            throw new IllegalArgumentException("Данный UUID зарезервирован");
        }
        this.uuid = uuid;
        uuidsForCurrentHost.add(uuid);
        try (Jedis jedis = jedisPool.getResource())
        {
            String countUsers = jedis.hget(REDIS_MAP_SHARED_POOL, uuid);
            int newValue = countUsers != null ? Integer.parseInt(countUsers) + 1 : 1;
            jedis.hset(REDIS_MAP_SHARED_POOL, uuid, Integer.toString(newValue));
        }
        cleaner.register(this, () -> RedisMap.removeByUUID(uuid));
    }

    private static void removeByUUID(String... uuids)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            for (String uuid : uuids)
            {
                String countUser = jedis.hget(REDIS_MAP_SHARED_POOL, uuid);
                int newValueCountUser = countUser != null ? Integer.parseInt(countUser) - 1 : 0;
                if (newValueCountUser == 0)
                {
                    Transaction tr = jedis.multi();
                    tr.del(uuid);
                    tr.hdel(REDIS_MAP_SHARED_POOL, uuid);
                    tr.exec();
                }
                else
                {
                    jedis.hset(REDIS_MAP_SHARED_POOL, uuid, Integer.toString(newValueCountUser));
                }
            }
        }
    }

    @Override
    public int size()
    {
        return keySet().size();
    }

    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }

    @Override
    public boolean containsKey(final Object key)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hexists(uuid, key.toString());
        }
    }

    @Override
    public boolean containsValue(final Object value)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hvals(uuid).contains(value);
        }
    }

    @Override
    public String get(final Object key)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hget(uuid, key.toString());
        }
    }

    @Override
    public String put(final String key, final String value)
    {
        if (value == null || key == null)
        {
            throw new NullPointerException(ERROR_NULL_MESSAGE);
        }
        try (Jedis jedis = jedisPool.getResource())
        {
            String prevValue = null;
            Transaction tr = jedis.multi();
            tr.hget(uuid, key);
            tr.hset(uuid, key, value);
            prevValue = (String) tr.exec().get(0);
            return prevValue;
        }
    }

    @Override
    public String remove(final Object key)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            Transaction tr = jedis.multi();
            tr.hget(uuid, key.toString());
            tr.hdel(uuid, key.toString());
            return (String) tr.exec().get(0);
        }
    }

    public void remove(String... keys)
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.hdel(uuid, keys);
        }
    }

    @Override
    public void putAll(final Map<? extends String, ? extends String> m)
    {
        Optional<? extends Entry<? extends String, ? extends String>> res = m.entrySet().stream()
                .filter(entry -> entry.getKey() == null || entry.getValue() == null)
                .findAny();
        if (!res.isEmpty())
        {
            throw new NullPointerException(ERROR_NULL_MESSAGE);
        }
        try (Jedis jedis = jedisPool.getResource())
        {
            jedis.hset(uuid, (Map<String, String>)m);
        }
    }

    @Override
    public void clear()
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            log.info(String.format("delete all data for key=%s", uuid));
            jedis.del(uuid);
        }
    }

    @Override
    public Set<String> keySet()
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hkeys(uuid);
        }
    }

    @Override
    public Collection<String> values()
    {
        try (Jedis jedis = jedisPool.getResource())
        {
            return jedis.hvals(uuid);
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet()
    {
        Set<Entry<String, String>> result;
        try (Jedis jedis = jedisPool.getResource())
        {
            RedisEntrySet asd = new RedisEntrySet(jedis.hgetAll(uuid).entrySet());
            return asd;
        }
    }

    public String getUuid()
    {
        return uuid;
    }

    @Override
    public void clean()
    {
        log.info(String.format("clear data for uuid=%s", uuid));
        clear();
    }

    private final class RedisEntrySet extends AbstractSet<Entry<String, String>>
    {

        /**
         * Хотелось бы использовать scan операцию, но порой из-за оптимизаций он возвращает все значения
         * https://redis.io/commands/scan
         */

        Set<Entry<String, String>> entrySet;

        private RedisEntrySet(Set<Entry<String, String>> entry)
        {
            entrySet = entry.stream()
                    .map(e -> new Node(e.getKey(), e.getValue()))
                    .collect(Collectors.toSet());
        }

        @Override
        public void clear()
        {
            RedisMap.this.clear();
            entrySet.clear();
        }

        @Override
        public Iterator<Entry<String, String>> iterator()
        {
            return entrySet.iterator();
        }

        @Override
        public int size()
        {
            return entrySet.size();
        }

        @Override
        public boolean contains(Object o)
        {
            if (!(o instanceof Node))
            {
                return false;
            }
            return entrySet.contains(o);
        }

        @Override
        public boolean remove(Object o)
        {
            if (!(o instanceof Node))
            {
                return false;
            }
            Node n = (Node)o;
            if (entrySet.contains(n))
            {
                RedisMap.this.remove(n.getKey());
                return entrySet.remove(n);
            }
            return false;
        }
    }

    private final class Node implements Map.Entry<String, String>
    {
        private final String key;
        private String value;

        private Node(String key, String value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey()
        {
            return key;
        }

        @Override
        public String getValue()
        {
            return value;
        }

        @Override
        public String setValue(String value)
        {
            return RedisMap.this.put(key, value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            Node node = (Node)o;
            return Objects.equals(key, node.key) &&
                    Objects.equals(value, node.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public String toString()
        {
            return key + "=" + value;
        }
    }
}