/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 如果有两个线程同时调用waitForRunning()，第三个线程调用wakeup()，那此时两个线程都被允许继续执行，那么这是expected behaviour嘛”
 * 我的拙见是：waitForRunning是为了阻塞自身thread而设计的
 * 如果是上层代码来调用，那就是别的线程来调用了，这样才会出现多个线程同时调用waitForRunning
 * 所以我理解为waitForRunning是被ServiceThread中的thread变量的run方法执行的，wakeup是其他线程（比如主线程）来唤醒thread变量的。
 */
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    private Thread thread;

    /**
     * 既能定时执行任务
     * 又能通过其他显式调用方法"提前执行任务"
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /**
     * 保证当调用waitForRunning()时，若发现已有线程调用了wakeup()，
     * 但waitPoint还没被释放时（count还不为0），可立即退出（因为马上waitPoint就要被释放了）
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * wakeup不需要原子性的原因在于，stopped被置True后，是允许上层代码继续执行 waitForRunning()的，
     * 即停了之后，还跑不跑waitForRunning()，其实完全由上层代码决定。
     * stopped 只是提供给上层一个"君子协议"一样的标志位：有人告诉我应该stop了，所以你最好check一下是不是stopped了。
     *  如stopped了，就不要再调用waitForRunning()了。stopped并不在waitForRunning()里起任何作用。
     */
    protected volatile boolean stopped = false;
    protected boolean isDaemon = false;

    /**
     * 使起能重启线程
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     *
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 保证了不管调用多少次start()方法，只会有一个线程在运行
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        // 就不需要在外部创建线程
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                // 等待子线程执行（有执行超时时间 JOIN_TIME）
                // 超时后执行下面的业务代码
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    /**
     * 不执行任何任务。
     * 当不处于stopped状态，用户调用waitForRunning()可启动一段定时器，并阻塞一段时间
     *                    或使用wakeup()立即结束跑完当前的定时器，立即退出阻塞状态
     * @param interrupt
     */
    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 不是atomic：
     * 1. 先将hasNotified设置为true,告诉其他人，我马上就要把count减为0了（即释放这个CountDownLatch）
     * 2. 再把count减为0
     */
    public void wakeup() {
        /**
         * false：
         *  1. 要么没有其他线程执行过waitForRunning()
         *  2. 要么有其他线程执行了waitForRunning()，但没有任何wakeup()
         * 这时会继续执行到waitPoint.reset()，即使之前有人调用过waitForRunning()，现在也给它reset()，重新计时
         *
         * true：已有一个wakeup()或stop()被执行了，于是此次调用把hasNotified置false，清空标志位
         * 下次的waitForRunning就能正常进行了。假设hasNotified=true，但wakeup()里的waitPoint.countDown();仍没有执行
         * 所以理论上正在执行waitPoint.await(...)的前一个线程没有被释放，而立刻调用waitForRunning()的线程
         * 因为看到hasNotified=true之后，将hasNotified置false就退出了
         *
         */
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    protected void waitForRunning(long interval) {
        /**
         * true，说明有其它线程调用该实例的wakeup()或stop()方法
         *
         *     // 假设我们创建了一个 XxxServiceThread xxx = new XxxServiceThread()
         *     // 线程A持有 xxx 实例
         *     // 线程B持有 xxx 实例
         *     // case1:
         *     // 时间片1，线程A.xxx调用了 wakeup() 设置了 hasNotified = True，方法结束
         *     // 时间片2，线程B.xxx调用了waitForRunning(..)，进入if，return
         *     //
         *     // case2:
         *     // 时间片1，线程A.xxx调用shutdown()，设置了 hasNotified = True，设置stopped标志位，方法结束
         *     // 时间片2，线程B.xxx调用了waitForRunning(...)，进入if，return
         *     //
         *     // case3:
         *     // 线程C也持有 xxx 实例
         *     // 时间片1，线程A.xxx调用了wakeup() 设置 hasNotified = True，但还没来的及 waitPoint.countDown()
         *     // 时间片2，线程B.xxx调用了 waitForRunning(...)，设置hasNotified=false，进入if，返回。直接业务代码
         *     // 时间片3，线程C.xxx调用了 waitForRunning(...)，waitPoint.reset(), waitPoint.await(...)，进入等待
         *     // 时间片4，线程A.xxx 执行到了 waitPoint.countDown()，此时线程C.xxx还没等多久，就继续下面的业务代码了，在时间上可能造成 线程B 的业务代码和线C 的业务代码重合执行。
         *     // 以上只是假设，但 waitForRunning 首先只能在 ServiceThread 子类中调用，一般就在 run 方法中使用
         *     且 ServiceThread 的 start 方法也保证了只有一个线程运行
         *     也就保证了 ServiceThread 子类的实例的 run 方法只会在一个线程中执行
         *     即 run 方法中存在 waitForRunning 调用，也只会由一个线程触发。
         */
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
