package cc.wang1.component.redis;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis 客户端
 * @author wang1
 * @param <T> 值类型
 */
@SuppressWarnings("all")
public interface RedisClient<T> {

    /**
     * 删除键
     * commit 全量清除缓存数据
     */
    long del(String key);

    /**
     * 新增
     */
    long zAdd(String zSet, long score, T value);

    /**
     * 获取延迟队列中 到期/已到期 的第一个消息
     */
    T zFirst(String zSet);

    /**
     * 添加到期消息
     */
    long lPush(String list, T value);

    /**
     * 批量添加到期消息
     */
    long lPush(String list, List<T> valueList);

    /**
     * commit 提交单个消息时删除缓存
     */
    long lRem(String list, long count, T value);

    /**
     * 获取未提交的消息数量
     */
    long lLen(String list);

    /**
     * 获取指定范围内未提交的消息数量
     */
    List<T> lRange(String list, long start, long end);

    /**
     * commit 提交指定范围内的消息
     */
    long lTrim(String list, long start, long end);

    /**
     * 用于阻塞等待拉取消息
     */
    T bLMove(String source, String destination, long timeout, TimeUnit unit);

    /**
     * 到期消息转移脚本
     * @return long  =-1: 延迟队列为空；>=0: 距今最近的一条消息的到期时间
     */
    long executeTransferScript(String script, List<String> keys, List<Long> args);

    /**
     * 批量添加延迟消息
     * @param valueList [score1, value1, score2, value2, ···] 妥善处理参数序列化
     */
    long executeBatchAddScript(String script, List<String> keys, List<Object> valueList);

    /**
     * 移除延迟消息
     */
    long executeRemoveScript(String script, List<String> keys, T data);

    /**
     * 批量移除延迟消息
     */
    long executeBatchRemoveScript(String script, List<String> keys, List<T> valueList);

    /**
     * 消息拉取操作
     */
    List<T> executePollScript(String script, List<String> keys, long batchSize);
}
