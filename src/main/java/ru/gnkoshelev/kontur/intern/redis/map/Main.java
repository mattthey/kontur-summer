package ru.gnkoshelev.kontur.intern.redis.map;



import java.util.Collection;
import java.util.Map;

public class Main
{
    public static void main(String[] args) {
        int n = 1000;
        RedisMap rm = new RedisMap();
        for (int i = 0; i < n; i++) {
            String ii = Integer.toString(i);
            rm.put(ii, Integer.toString(i + 9));
        }
        Long t = System.currentTimeMillis();
        System.out.println(rm.containsValue(Integer.toString(n + 8)));
        System.out.println(System.currentTimeMillis() - t);
    }

    public static void test(Map<String, String> map)
    {
        println("test " + map.getClass().getSimpleName());
        println("isEmpty() " + map.isEmpty());
        println("put() " + map.put("1", "asd"));
        println("remove() " + map.remove("2"));
        println("remove() " + map.remove("1"));
        println("isEmpty()" + map.isEmpty());
    }

    public static void println(Object o) {
        System.out.println(o);
    }
    public static <A> void printCollections(Collection<A> collection) {
        for (A item : collection)
            System.out.println(item.toString());
    }
}
