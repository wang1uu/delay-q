package cc.wang1.test;

import cc.wang1.component.RedisDelayQueue;
import cc.wang1.component.redis.RedisClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SpringBootTest
public class TestMain {

    @Resource
    private RedisClient<String> stringRedisClient;

    @Test
    public void test01() {
        RedisDelayQueue<String> redisDelayQueue = new RedisDelayQueue<>(stringRedisClient, "test01", TimeUnit.SECONDS.toMillis(3), 20);
        // redisDelayQueue.producer("p1").offer(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10), "asdasd");




        LockSupport.park();
    }
}
