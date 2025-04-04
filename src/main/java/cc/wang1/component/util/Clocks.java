package cc.wang1.component.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 使用ScheduledExecutorService实现高并发场景下System.currentTimeMillis()的性能问题的优化
 * @author wang1
 */
public enum Clocks {
    INSTANCE(1);

    /**
     * 更新周期
     */
    private final long period;
    /**
     * 缓存时间戳
     */
    private final AtomicLong currentMillis;
    /**
     * 更新线程
     */
    private final ScheduledExecutorService executorService;

    Clocks(long period) {
        this.period = period;
        this.currentMillis = new AtomicLong(System.currentTimeMillis());

        this.executorService = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread systemClock = new Thread(runnable, "System-Clock-Update");
            systemClock.setDaemon(true);

            return systemClock;
        });

        executorService.scheduleAtFixedRate(
                () -> currentMillis.set(System.currentTimeMillis()),
                this.period,
                this.period,
                TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(executorService::shutdown, "System-Clock-Release"));
    }

    public long currentTimeMillis() {
        return currentMillis.get();
    }

    public long currentTimeMillis(long offset) {
        return currentMillis.get() + offset;
    }
}
