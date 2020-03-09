package ru.gnkoshelev.kontur.intern.redis.map;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author Gregory Koshelev
 * @updated Matvey Noskov
 */
public class RedisMapTest {

    private static final String ERROR_NULL_MESSAGE = "Не поддерживается значение или ключ null";

    @Test
    public void baseTests() {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new RedisMap();

        map1.put("one", "1");

        map2.put("one", "ONE");
        map2.put("two", "TWO");

        Assert.assertEquals("1", map1.get("one"));
        Assert.assertEquals(1, map1.size());
        Assert.assertEquals(2, map2.size());

        map1.put("one", "first");

        Assert.assertEquals("first", map1.get("one"));
        Assert.assertEquals(1, map1.size());

        Assert.assertTrue(map1.containsKey("one"));
        Assert.assertFalse(map1.containsKey("two"));

        Set<String> keys2 = map2.keySet();
        Assert.assertEquals(2, keys2.size());
        Assert.assertTrue(keys2.contains("one"));
        Assert.assertTrue(keys2.contains("two"));

        Collection<String> values1 = map1.values();
        Assert.assertEquals(1, values1.size());
        Assert.assertTrue(values1.contains("first"));
    }

    @Test
    public void advancedTests()
    {
        Map<String, String> map1 = new RedisMap();
        Map<String, String> map2 = new RedisMap();
        Map<String, String> templateMap = Map.of("1", "2", "3", "4");
        map1.putAll(templateMap);
        map2.putAll(Map.of("5", "6", "7", "8"));

        Assert.assertTrue("Не найдено существующее значение", map2.containsValue("6"));
        Assert.assertFalse("Найдено несуществующее значение", map2.containsValue("5"));

        Assert.assertEquals("Множество ключей не совпадает с ожидаемым", templateMap.keySet(), map1.keySet());
        Assert.assertEquals("Множество значений не совпадает с ожидаемым", Set.copyOf(templateMap.values()), Set.copyOf(map1.values()));

        Assert.assertEquals("После добавления нескольких элементов", 2, map1.size());
        Assert.assertEquals("После удаления элемента по ключу должно вернуться значение", "2", map1.remove("1"));
        Assert.assertNull("После добавления нового ключа возвращается null", map1.put("9", "10"));
        Assert.assertEquals("После замены значения по ключу возвращается старое значение", "10", map1.put("9", "11"));

        Assert.assertEquals("Размер массива не совпадает с ожидаемым после удаления элемента", 2, map1.size());
        Assert.assertNull("Получено значение по удаленному ключу", map1.get("1"));

        Assert.assertFalse("Не пустая мапка оказалась пустой", map2.isEmpty());
        map2.clear();
        Assert.assertEquals("После удаления всех элементов размер массива не изменился", 0, map2.size());
        Assert.assertNull("Получено значение по удаленному ключу", map2.get("5"));
        Assert.assertNull("Получено значение по удаленному ключу", map2.get("6"));
        Assert.assertTrue("После удаления всех элементов мапка должна быть пустой", map2.isEmpty());
    }

    @Test
    public void testEntrySet()
    {
        Map<String, String> map = new RedisMap();
        Map<String, String> templateMap = Map.of("1", "2", "3", "4", "5", "6", "7", "8");
        map.putAll(templateMap);
        Assert.assertEquals("Размеры должны совпадать", templateMap.size(), map.size());
        for (String k : templateMap.keySet())
        {
            Assert.assertEquals("Значение не совпадает с ожидаемым", templateMap.get(k), map.get(k));
        }

        Set<Entry<String, String>> redisEntrySet = map.entrySet();
        redisEntrySet.forEach(entry -> {
            entry.setValue(entry.getKey());
        });
        for (String k : map.keySet())
        {
            Assert.assertEquals("Значение не совпадает с ожидаемым", k, map.get(k));
        }
        redisEntrySet.clear();
        Assert.assertEquals("Данные должны уменьшиться", 0, map.size());
    }

    @Test(expected = NullPointerException.class)
    public void testNullValue()
    {
        Map<String, String> map = new RedisMap();
        try {
            map.put("1", null);
        } catch (Exception ex) {
            Assert.assertEquals("Сообщения об ошибке не совпадают", ERROR_NULL_MESSAGE, ex.getMessage());
            throw ex;
        }
    }

    @Test(expected = NullPointerException.class)
    public void testNullKey()
    {
        Map<String, String> map = new RedisMap();
        try {
            map.put(null, "0");
        } catch (Exception ex) {
            Assert.assertEquals("Сообщения об ошибке не совпадают", ERROR_NULL_MESSAGE, ex.getMessage());
            throw ex;
        }
    }

    @Test(expected = NullPointerException.class)
    public void testPutAllWithNull()
    {
        HashMap<String, String> templateMap = new HashMap<>();
        templateMap.put("1", null);

        Map<String, String> map = new RedisMap();
        try {
            map.putAll(templateMap);
            throw new Exception("Ошибка должна упасть раньше");
        } catch (Exception ex) {
            Assert.assertEquals("Сообщения об ошибке не совпадают", ERROR_NULL_MESSAGE, ex.getMessage());
        }

        templateMap.clear();
        templateMap.put(null, "123");

        try {
            map.putAll(templateMap);
        } catch (Exception ex) {
            Assert.assertEquals("Сообщения об ошибке не совпадают", ERROR_NULL_MESSAGE, ex.getMessage());
            throw ex;
        }
    }

}
