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
                    "local increment = tonumber(ARGV[2]) " +
                    "local limit = T + 1000 " +
                    "local batch_size = tonumber(ARGV[3]) " +
                    "while T < limit do " +
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
                    "    T = T + increment " +
                    "    if T >= limit then break end " +
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
                    "local timeout = tonumber(ARGV[1]) " +
                    "local result = redis.call('BLPOP', expired_list, timeout) " +
                    "if result then " +
                    "    local element = result[2] " +
                    "    redis.call('RPUSH', cached_list, element) " +
                    "    return element " +
                    "end " +
                    "return nil ";

    /**
     * 批量拉取消息 lua脚本
     */
    public static final String BATCH_POLL_SCRIPT =
                    "local expired_list = KEYS[1] " +
                    "local cached_list = KEYS[2] " +
                    "local script_timeout = tonumber(ARGV[1]) " +
                    "local max_elements = tonumber(ARGV[2]) " +
                    "local elements = {} " +
                    "local i = 0 " +
                    "local start_time = redis.call('TIME')[1] " +
                    "while i < max_elements do " +
                    "    local current_time = redis.call('TIME')[1] " +
                    "    local elapsed_time = current_time - start_time " +
                    "    local remaining_time = script_timeout - elapsed_time " +
                    "    if remaining_time <= 0 then " +
                    "        break " +
                    "    end " +
                    "    local result = redis.call('BLPOP', expired_list, remaining_time) " +
                    "    if result then " +
                    "        local element = result[2] " +
                    "        redis.call('RPUSH', cached_list, element) " +
                    "        table.insert(elements, element) " +
                    "        i = i + 1 " +
                    "    else " +
                    "        break " +
                    "    end " +
                    "end " +
                    "return elements";
}
