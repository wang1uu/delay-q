package cc.wang1.component.common;

/**
 * 常量
 * @author wang1
 */
public final class DelayQueueConstants {
    private DelayQueueConstants() {}

    /**
     * 分隔符
     */
    public static final String MARK_SEPARATOR = "-";

    /**
     * 延迟队列后缀
     */
    public static final String DELAY_ZSET_SUFFIX = "-delay-zset";

    /**
     * 到期消息列表后缀
     */
    public static final String EXPIRED_LIST_SUFFIX = "-expired-list";

    /**
     * 缓存消息列表后缀
     */
    public static final String CACHED_LIST_SUFFIX = "-cached-list";

    /**
     * 生产者后缀
     */
    public static final String PRODUCER_SUFFIX = "-producer";

    /**
     * 消费者后缀
     */
    public static final String CONSUMER_SUFFIX = "-consumer";

    /**
     * 到期消息转移lua脚本
     */
    public static final String TRANSFER_SCRIPT =
                    "local zset_key = KEYS[1] " +
                    "local list_key = KEYS[2] " +
                    "local T = tonumber(ARGV[1]) " +
                    "local max_duration = tonumber(ARGV[2]) " +
                    "local batch_size = tonumber(ARGV[3]) " +
                    "local start_time = redis.call('TIME') " +
                    "local start_timestamp = tonumber(start_time[1]) " +
                    "local current_timestamp = tonumber(start_time[1]) " +
                    "while current_timestamp < start_timestamp + max_duration do " +
                    "    local elements = redis.call('ZRANGE', zset_key, '-inf', T, 'BYSCORE', 'WITHSCORES', 'LIMIT', 0, batch_size) " +
                    "    if #elements <= 0 then break end " +
                    "    local to_remove = {} " +
                    "    for i = 1, #elements, 2 do " +
                    "        table.insert(to_remove, elements[i]) " +
                    "    end " +
                    "    if #to_remove > 0 then " +
                    "        redis.call('ZREM', zset_key, unpack(to_remove)) " +
                    "        redis.call('RPUSH', list_key, unpack(to_remove)) " +
                    "    end " +
                    "    local current_time = redis.call('TIME') " +
                    "    local current_timestamp = tonumber(current_time[1]) " +
                    "end " +
                    "local first_element = redis.call('ZRANGE', zset_key, 0, 0, 'WITHSCORES') " +
                    "if #first_element > 0 then " +
                    "    return tonumber(first_element[2]) " +
                    "else " +
                    "    return -1 " +
                    "end;";

    /**
     * 批量新增消息 lua脚本
     */
    public static final String BATCH_ADD_SCRIPT =
                    "local zset_key = KEYS[1] " +
                    "local scores_and_elements = ARGV " +
                    "return redis.call('ZADD', zset_key, unpack(scores_and_elements));";

    /**
     * 删除消息 lua脚本
     */
    public static final String REMOVE_SCRIPT =
                    "local zset_key = KEYS[1] " +
                    "local list_key = KEYS[2] " +
                    "local element = ARGV[1] " +
                    "local removed_from_zset = redis.call('ZREM', zset_key, element) " +
                    "if removed_from_zset == 1 then " +
                    "    return 1 " +
                    "end " +
                    "return redis.call('LREM', list_key, 0, element) ";

    /**
     * 批量删除消息 lua脚本
     */
    public static final String BATCH_REMOVE_SCRIPT =
                    "local zset_key = KEYS[1] " +
                    "local list_key = KEYS[2] " +
                    "local elements = ARGV " +
                    "local removed = redis.call('ZREM', zset_key, unpack(elements)) " +
                    "return removed + redis.call('LREM', list_key, 0, unpack(elements)) ";

    /**
     * 拉取消息 lua脚本
     */
    public static final String POLL_SCRIPT =
                    "local expired_list = KEYS[1] " +
                    "local cached_list = KEYS[2] " +
                    "local batch_size = tonumber(ARGV[1]) " +
                    "local elements = redis.call('LPOP', expired_list, batch_size) " +
                    "if type(elements) == 'table' then " +
                    "    redis.call('RPUSH', cached_list, unpack(elements)) " +
                    "    return elements " +
                    "elseif elements then " +
                    "    redis.call('RPUSH', cached_list, elements) " +
                    "    return {elements} " +
                    "end " +
                    "return {} ";
}
