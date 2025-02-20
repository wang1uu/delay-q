package cc.wang1.component.redis;

import java.util.List;

/**
 * Redis 客户端
 * @author wang1
 * @param <T> 值类型
 */
public interface RedisClient<T> {

    long del(String key);

    /**
     * 新增
     */
    long zAdd(String zSet, long score, T value);
    long executeBatchAddScript(String script, List<String> keys, List<Object> valueList);

    /**
     * 获取延迟队列中 到期/已到期 的第一个消息
     */
    T zFirst(String zSet);

    /**
     * 添加到期消息
     */
    long lPush(String list, T value);
    long lPush(String list, List<T> valueList);
    long lRem(String list, long count, T value);
    long lLen(String list);
    List<T> lRange(String list, long start, long end);
    long lTrim(String list, long start, long end);

    /**
     * 到期消息转移脚本
     * @return long  =-1: 延迟队列为空；>=0: 距今最近的一条消息的到期时间
     */
    long executeTransferScript(String script, List<String> keys, List<Long> args);

    /**
     * 移除
     */
    long executeRemoveScript(String script, List<String> keys, T data);
    long executeBatchRemoveScript(String script, List<String> keys, List<T> valueList);

    /**
     * 消费
     */
    T executePollScript(String script, List<String> keys, List<Object> valueList);
    List<T> executeBatchPollScript(String script, List<String> keys, List<Object> valueList);
}
