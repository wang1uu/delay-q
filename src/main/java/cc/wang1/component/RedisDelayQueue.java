package cc.wang1.component;

import cc.wang1.component.common.DelayQueueConstants;
import cc.wang1.component.redis.RedisClient;
import cc.wang1.component.util.Clocks;
import cc.wang1.component.util.Logs;
import lombok.Getter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 延迟队列
 * @author wang1
 * @param <T> 消息数据类型
 */
public class RedisDelayQueue<T> {
    /**
     * Redis客户端
     */
    private final RedisClient<T> redisClient;

    /**
     * 队列名称
     */
    @Getter
    private final String name;

    /**
     * 未到期消息 zset
     */
    private final String delayZSet;

    /**
     * 到期消息 list
     */
    private final String expiryList;

    /**
     * 每批处理的到期元素个数
     */
    @Getter
    private final long batchSize;

    /**
     * 最大处理时间(ms)
     */
    @Getter
    private final long maxProcessTime;

    /**
     * 到期消息转移线程
     */
    private final AtomicReference<Thread> transferThread;

    /**
     * 生产者列表
     */
    @Getter
    private final ConcurrentMap<String, Producer> producers = new ConcurrentHashMap<>();

    /**
     * 消费者列表
     */
    @Getter
    private final ConcurrentMap<String, Consumer> consumers = new ConcurrentHashMap<>();


    public RedisDelayQueue(RedisClient<T> redisClient,
                           String name,
                           long maxProcessTime,
                           long batchSize) {
        this.redisClient = redisClient;
        this.name = name;
        this.delayZSet = name  + DelayQueueConstants.DELAY_ZSET_SUFFIX;
        this.expiryList = name + DelayQueueConstants.EXPIRED_LIST_SUFFIX;
        this.batchSize = batchSize;
        this.maxProcessTime = maxProcessTime;
        this.transferThread = new AtomicReference<>(createTransferThread());
        this.transferThread.get().start(); // start transfer expired item
    }

    /**
     * 构建到期消息处理线程
     */
    private Thread createTransferThread() {
        Thread thread = new Thread(this::transferTask);
        thread.setDaemon(false);
        thread.setName(name + "-Expired-Item-Processor-" + Clocks.INSTANCE.currentTimeMillis());

        thread.setUncaughtExceptionHandler((t, e) -> Logs.error("Expired-Item-Processor encountered an uncaught exception: ", e));
        return thread;
    }

    /**
     * 到期消息转移逻辑
     */
    private void transferTask() {
        while (true) {
            long start = 0, end = 0, scriptFinish = 0;
            try {
                Logs.info("Transfer task start at " + (start = Clocks.INSTANCE.currentTimeMillis()));
                // 转移到期元素
                // 获取下一个最近到期的元素
                long timeout = redisClient.executeTransferScript(DelayQueueConstants.TRANSFER_SCRIPT,
                        Arrays.asList(delayZSet, expiryList),
                        Arrays.asList(Clocks.INSTANCE.currentTimeMillis(),
                        maxProcessTime,
                        batchSize));
                Logs.info("transfer script running finish at " + (scriptFinish = Clocks.INSTANCE.currentTimeMillis()) + " cost [" + (scriptFinish - start) + "ms] " + " timeout => " + timeout);

                // 延迟消息队列为空
                if (timeout < 0) {
                    Logs.info("transfer executor park at " + Clocks.INSTANCE.currentTimeMillis());
                    LockSupport.park(this);
                    Logs.info("transfer executor continue running at " + Clocks.INSTANCE.currentTimeMillis());
                }

                // park到下一个最近到期的元素
                long parkMillis = 0;
                if (timeout > 0 && (parkMillis = timeout-Clocks.INSTANCE.currentTimeMillis()) > 0) {
                    Logs.info("transfer executor will park [" + parkMillis + "ms] at " + Clocks.INSTANCE.currentTimeMillis());
                    LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(parkMillis));
                    Logs.info("transfer executor continue running at " + Clocks.INSTANCE.currentTimeMillis());
                }
            }catch (Exception e) {
                Logs.error("transfer task fail at " + Clocks.INSTANCE.currentTimeMillis(), e);
                LockSupport.parkNanos(this, TimeUnit.SECONDS.toNanos(1));
            }
        }
    }

    /**
     * 删除
     * @author wang1
     */
    public long remove(T data) {
        if (data == null) {
            return 0;
        }
        return redisClient.executeRemoveScript(DelayQueueConstants.REMOVE_SCRIPT, Arrays.asList(delayZSet, expiryList), data);
    }
    public long remove(List<T> dataList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return 0;
        }
        return redisClient.executeBatchRemoveScript(DelayQueueConstants.BATCH_REMOVE_SCRIPT, Arrays.asList(delayZSet, expiryList), dataList);
    }

    /**
     * 填充生产者名称
     */
    public String popularProducerName(String name) {
        return RedisDelayQueue.this.name + DelayQueueConstants.MARK_SEPARATOR + name + DelayQueueConstants.PRODUCER_SUFFIX;
    }

    /**
     * 填充消费者名称
     */
    public String popularConsumerName(String name) {
        return RedisDelayQueue.this.name + DelayQueueConstants.MARK_SEPARATOR + name + DelayQueueConstants.CONSUMER_SUFFIX;
    }

    /**
     * 获取生产者
     * @param name 生产者标识
     * @author wang1
     */
    public Producer producer(String name) {
        if (!StringUtils.hasText(name)) {
            throw new IllegalArgumentException("producer name is empty.");
        }

        String producerName = popularProducerName(name);

        Producer producer = producers.getOrDefault(producerName, new Producer(name, redisClient, this));
        producers.putIfAbsent(producerName, producer);
        return producer;
    }

    /**
     * 获取消费者
     * @param name 消费者名称
     * @param autoCommitInterval(s) <=0: 关闭自动提交位移 >0: 自动提交消费位移的时间间隔
     * @author wang1
     */
    public Consumer consumer(String name, long autoCommitInterval) {
        if (!StringUtils.hasText(name)) {
            throw new IllegalArgumentException("consumer name is empty.");
        }

        String consumerName = popularConsumerName(name);

        Consumer consumer = consumers.getOrDefault(consumerName, new Consumer(name,
                redisClient,
                this,
                autoCommitInterval > 0,
                autoCommitInterval > 0 ? TimeUnit.SECONDS.toMillis(autoCommitInterval) : 0));
        consumers.putIfAbsent(consumerName, consumer);
        return consumer;
    }

    /**
     * 释放
     * @author wang1
     */
    public void close() {

    }


    /**
     * 生产者
     * @author wang1
     */
    public class Producer {
        @Getter
        private final String name;

        @Getter
        private final RedisDelayQueue<T> redisDelayQueue;

        private final RedisClient<T> redisClient;

        private Producer(String name, RedisClient<T> redisClient, RedisDelayQueue<T> redisDelayQueue) {
            this.name = name;
            this.redisClient = redisClient;
            this.redisDelayQueue = redisDelayQueue;
        }

        /**
         * 新增
         * @author wang1
         * @param expiry 到期时间戳
         * @param data 消息内容
         */
        public long offer(long expiry, T data) {
            if (data == null) {
                return 0;
            }

            if (expiry <= Clocks.INSTANCE.currentTimeMillis()) {
                redisClient.lPush(expiryList, data);
                return 1;
            }

            long result = redisClient.zAdd(delayZSet, expiry, data);
            if (result <= 0) {
                return 0;
            }

            // 添加成功
            // 检查当前队头元素是否有变化
            T first = redisClient.zFirst(delayZSet);
            if (!Objects.equals(first, data)) {
                return 1;
            }

            Logs.info("Head item change to " + data);
            LockSupport.unpark(transferThread.get());

            return 1;
        }

        /**
         * 批量新增
         * @author wang1
         */
        public long offer(Map<T, Long> dataMap) {
            if (CollectionUtils.isEmpty(dataMap)) {
                return 0;
            }

            ArrayList<T> expiredDataList = new ArrayList<>();
            ArrayList<Object> delayDataList = new ArrayList<>();

            dataMap.forEach((k, v) -> {
                if (k == null || v == null) {
                    return;
                }

                if (Clocks.INSTANCE.currentTimeMillis() >= v) {
                    expiredDataList.add(k);
                }else {
                    delayDataList.add(v);
                    delayDataList.add(k);
                }
            });

            redisClient.lPush(expiryList, expiredDataList);
            redisClient.executeBatchAddScript(DelayQueueConstants.BATCH_ADD_SCRIPT, Collections.singletonList(delayZSet), delayDataList);

            // 更新park时间
            LockSupport.unpark(transferThread.get());

            return expiredDataList.size() + delayDataList.size()/2;
        }
    }


    /**
     * 消费者
     * @author wang1
     */
    public class Consumer {
        @Getter
        private final String name;
        
        private final String cachedList;

        @Getter
        private final RedisDelayQueue<T> redisDelayQueue;

        private final RedisClient<T> redisClient;

        /**
         * 上次全量提交消费位移的时间戳
         */
        @Getter
        private long lastCommitedTime;

        @Getter
        private final boolean autoCommit;

        /**
         * 自动提交位移时间间隔（ms）
         */
        @Getter
        private final long autoCommitInterval;

        /**
         * 防止并发操作
         */
        private final AtomicReference<Thread> currentThread = new AtomicReference<>(null);

        private Consumer(String name, RedisClient<T> redisClient, RedisDelayQueue<T> redisDelayQueue, boolean autoCommit, long autoCommitInterval) {
            this.name = name;
            this.redisDelayQueue = redisDelayQueue;
            this.redisClient = redisClient;
            this.autoCommit = autoCommit;
            this.autoCommitInterval = autoCommitInterval;
            this.cachedList = popularConsumerName(name) + DelayQueueConstants.CACHED_LIST_SUFFIX;
        }

        private void autoCommit() {
            if (autoCommit && Clocks.INSTANCE.currentTimeMillis() > lastCommitedTime + autoCommitInterval) {
                redisClient.del(cachedList);
                lastCommitedTime = Clocks.INSTANCE.currentTimeMillis();
            }
        }

        private void lock() {
            if (this.currentThread.get() != Thread.currentThread() && !this.currentThread.compareAndSet(null, Thread.currentThread())) {
                throw new RuntimeException("Concurrent operation failed, consumer has locked by another thread " + Optional.ofNullable(currentThread.get()).map(Thread::getName).orElse("null (previous thread has released)"));
            }
        }

        private void release() {
            if (!this.currentThread.compareAndSet(Thread.currentThread(), null)) {
                throw new RuntimeException("Concurrent operation failed, consumer has locked by another thread " + Optional.ofNullable(currentThread.get()).map(Thread::getName).orElse("null (previous thread has released)"));
            }
        }

        public List<T> poll(long batchSize, long timeout, TimeUnit unit) {
            lock();
            autoCommit();

            long current = Clocks.INSTANCE.currentTimeMillis();
            long deadline = unit.toMillis(timeout) + current;

            ArrayList<T> result = new ArrayList<>();

            while (Clocks.INSTANCE.currentTimeMillis() < deadline && result.size() < batchSize) {
                List<T> dataList = redisClient.executePollScript(DelayQueueConstants.POLL_SCRIPT,
                        Arrays.asList(expiryList, cachedList),
                        Collections.singletonList(batchSize - result.size()));

                result.addAll(dataList);
                if (result.size() >= batchSize) { // 理论上这里是不会出现大于的情况的
                    break;
                }

                // 阻塞等待
                long waitTime = deadline - Clocks.INSTANCE.currentTimeMillis();
                if (waitTime > 0) {
                    T data = redisClient.bLMove(expiryList, cachedList, waitTime, TimeUnit.MILLISECONDS);
                    if (data != null) {
                        result.add(data);
                    }
                }
            }

            release();
            return result;
        }

        public long getUnCommitedSize() {
            return redisClient.lLen(cachedList);
        }

        public List<T> listUnCommitedRange(long start, long end) {
            return redisClient.lRange(cachedList, start, end);
        }
        
        public void commit() {
            lock();

            redisClient.del(cachedList);
            lastCommitedTime = Clocks.INSTANCE.currentTimeMillis();

            release();
        }

        public void commit(T data) {
            lock();

            redisClient.lRem(cachedList, 0, data);

            release();
        }

        public void commit(long start, long end) {
            lock();

            redisClient.lTrim(cachedList, start, end);

            release();
        }
    }
}
