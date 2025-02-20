package cc.wang1.component.redis;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Redis 客户端适配器
 * @author wang1
 */
@Component(value = "stringRedisClient")
public class SpringStringRedisClientAdapter implements RedisClient<String> {

    @Resource(name = "delayQueueStringRedisClient")
    private RedisTemplate<String, String> redisTemplate;

    @Override
    public long del(String key) {
        return redisTemplate.delete(key) ? 1 : 0;
    }

    @Override
    public long zAdd(String zSet, long score, String value) {
        return Boolean.TRUE.equals(redisTemplate.opsForZSet().add(zSet, value, score)) ? 1 : 0;
    }

    @Override
    public long executeBatchAddScript(String script, List<String> keys, List<Object> dataList) {
        return redisTemplate.execute(new DefaultRedisScript<>(script, Long.class),
                new GenericToStringSerializer<>(Object.class),
                new GenericToStringSerializer<>(Long.class),
                keys,
                dataList.toArray());
    }

    @Override
    public String zFirst(String zSet) {
        Set<String> items = redisTemplate.opsForZSet().range(zSet, 0, 0);
        if (items == null || items.isEmpty()) {
            return null;
        }
        return items.iterator().next();
    }

    @Override
    public long lPush(String list, String value) {
        return redisTemplate.opsForList().rightPush(list, value);
    }

    @Override
    public long lPush(String list, List<String> valueList) {
        return redisTemplate.opsForList().rightPushAll(list, valueList);
    }

    @Override
    public long lRem(String list, long count, String value) {
        return redisTemplate.opsForList().remove(list, count, value);
    }

    @Override
    public long lLen(String list) {
        return redisTemplate.opsForList().size(list);
    }

    @Override
    public List<String> lRange(String list, long start, long end) {
        return redisTemplate.opsForList().range(list, start, end);
    }

    @Override
    public long lTrim(String list, long start, long end) {
        redisTemplate.opsForList().trim(list, start, end);
        return end - start;
    }

    @Override
    public String bLMove(String source, String destination, long timeout, TimeUnit unit) {
        return redisTemplate.opsForList().move(source, RedisListCommands.Direction.LEFT, destination, RedisListCommands.Direction.RIGHT, timeout, unit);
    }

    @Override
    public long executeTransferScript(String script, List<String> keys, List<Long> args) {
        return redisTemplate.execute(new DefaultRedisScript<>(script, Long.class),
                new GenericToStringSerializer<>(Long.class),
                new GenericToStringSerializer<>(Long.class),
                keys,
                args.toArray());
    }

    @Override
    public long executeRemoveScript(String script, List<String> keys, String data) {
        return redisTemplate.execute(new DefaultRedisScript<>(script, Long.class),
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                keys,
                data);
    }

    @Override
    public long executeBatchRemoveScript(String script, List<String> keys, List<String> dataList) {
        return redisTemplate.execute(new DefaultRedisScript<>(script, Long.class),
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                keys,
                dataList.toArray());
    }

    @Override
    public List<String> executePollScript(String script, List<String> keys, List<Object> valueList) {
        return redisTemplate.execute(new DefaultRedisScript<>(script, List.class),
                new GenericToStringSerializer<>(Long.class),
                new GenericToStringSerializer<>(List.class),
                keys,
                valueList.toArray());
    }
}
