package cc.wang1.test;

import cc.wang1.component.RedisDelayQueue;
import cc.wang1.component.redis.RedisClient;
import cc.wang1.component.util.Clocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SpringBootTest
public class TestMain {

    @Resource
    private RedisClient<String> stringRedisClient;

    private RedisDelayQueue<String> redisDelayQueue;

    @BeforeEach
    public void beforeAll() {
        this.redisDelayQueue = new RedisDelayQueue<>(stringRedisClient, "test01", TimeUnit.SECONDS.toMillis(10), 100);
    }

    @Test
    public void test01() {
        RedisDelayQueue<String>.Producer p1 = redisDelayQueue.producer("p1");
        p1.offer(Clocks.INSTANCE.currentTimeMillis(TimeUnit.SECONDS.toMillis(10)), "asdasd");

        HashMap<String, Long> dataMap = new HashMap<>();
        dataMap.put("+2", Clocks.INSTANCE.currentTimeMillis(TimeUnit.SECONDS.toMillis(2)));
        dataMap.put("-1", Clocks.INSTANCE.currentTimeMillis(TimeUnit.SECONDS.toMillis(-1)));
        dataMap.put("+10", Clocks.INSTANCE.currentTimeMillis(TimeUnit.SECONDS.toMillis(10)));
        p1.offer(dataMap);
    }

    @Test
    public void test02() {
        RedisDelayQueue<String>.Consumer c1 = redisDelayQueue.consumer("c1").autoCommit(3, TimeUnit.SECONDS);
        while (true) {
            System.out.println(Clocks.INSTANCE.currentTimeMillis());
            System.out.print(c1.getUnCommitedSize() + " ");
            System.out.println(c1.getLastCommitedTime());

            c1.poll(10, 2, TimeUnit.SECONDS).forEach(System.out::println);
        }
    }

    @Test
    public void test03() {
        RedisDelayQueue<String>.Consumer c1 = redisDelayQueue.consumer("c1");
        List<String> data = c1.poll(10, 300, TimeUnit.SECONDS);

        for (String x : data) {
            System.out.println(x);
            c1.commit(x);

            System.out.println(c1.getUnCommitedSize());
        }
    }

    @Test
    public void test04() {
        RedisDelayQueue<String>.Consumer c1 = redisDelayQueue.consumer("c1").autoCommit(3, TimeUnit.SECONDS);
        CompletableFuture.runAsync(() -> {
            while (true) {
                System.out.println(Clocks.INSTANCE.currentTimeMillis());
                c1.poll(10, 2, TimeUnit.SECONDS).forEach(System.out::println);
            }
        });

        RedisDelayQueue<String>.Producer p1 = redisDelayQueue.producer("p1");

        LockSupport.park();
    }

    @Test
    public void test05() {
        RedisDelayQueue<String>.Producer p1 = redisDelayQueue.producer("p1");
        RedisDelayQueue<String>.Consumer c1 = redisDelayQueue.consumer("c1").autoCommit(3, TimeUnit.SECONDS);

        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                List<String> data = c1.poll(10, 2, TimeUnit.SECONDS);
                System.out.println(1);
                data.forEach(s -> {
                    long currentTimeMillis = Clocks.INSTANCE.currentTimeMillis();
                    if (currentTimeMillis - Long.parseLong(s) > 5000) {
                        System.out.println("current => " + currentTimeMillis + " target => " + s + " r => " + (currentTimeMillis - Long.parseLong(s)));
                    }
                });
            }
        });

//        Executors.newSingleThreadExecutor().execute(() -> {
//            while (true) {
//                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
//                long expiry = Clocks.INSTANCE.currentTimeMillis(TimeUnit.SECONDS.toMillis(ThreadLocalRandom.current().nextInt(60)));
//                p1.offer(expiry, String.valueOf(expiry));
//            }
//        });

        LockSupport.park();
    }
}
