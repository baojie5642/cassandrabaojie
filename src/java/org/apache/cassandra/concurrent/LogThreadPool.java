/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apache.cassandra.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * This class encorporates some Executor best practices for Cassandra.  Most of the executors in the system
 * should use or extend this.  There are two main improvements over a vanilla TPE:
 * <p>
 * - If a task throws an exception, the default uncaught exception handler will be invoked; if there is
 * no such handler, the exception will be logged.
 * - MaximumPoolSize is not supported.  Here is what that means (quoting TPE javadoc):
 * <p>
 * If fewer than corePoolSize threads are running, the Executor always prefers adding a new thread rather than queuing.
 * If corePoolSize or more threads are running, the Executor always prefers queuing a request rather than adding a
 * new thread.
 * If a request cannot be queued, a new thread is created unless this would exceed maximumPoolSize, in which case,
 * the task will be rejected.
 * <p>
 * We don't want this last stage of creating new threads if the queue is full; it makes it needlessly difficult to
 * reason about the system's behavior.  In other words, if DebuggableTPE has allocated our maximum number of (core)
 * threads and the queue is full, we want the enqueuer to block.  But to allow the number of threads to drop if a
 * stage is less busy, core thread timeout is enabled.
 */

// 一个无论什么情况发生都会答印异常的线程池
public class LogThreadPool extends ThreadPoolExecutor implements BaojieExecutorService {
    protected static final Logger logger = LoggerFactory.getLogger(LogThreadPool.class);

    public static final RejectedExecutionHandler blockingExecutionHandler = new RejectedExecutionHandler() {
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            ((LogThreadPool) executor).onInitialRejection(task);
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (true) {
                if (executor.isShutdown()) {
                    ((LogThreadPool) executor).onFinalRejection(task);
                    throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
                }
                try {
                    if (queue.offer(task, 1000, TimeUnit.MILLISECONDS)) {
                        ((LogThreadPool) executor).onFinalAccept(task);
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        }
    };

    public LogThreadPool(String threadPoolName, int priority) {
        this(1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(threadPoolName, priority));
    }

    public LogThreadPool(int corePoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> queue, ThreadFactory factory) {
        this(corePoolSize, corePoolSize, keepAliveTime, unit, queue, factory);
    }

    public LogThreadPool(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        allowCoreThreadTimeOut(true);

        // block task submissions until queue has room.
        // this is fighting TPE's design a bit because TPE rejects if queue.offer reports a full queue.
        // we'll just override this with a handler that retries until it gets in.  ugly, but effective.
        // (there is an extensive analysis of the options here at
        //  http://today.java.net/pub/a/today/2008/10/23/creating-a-notifying-blocking-thread-pool-executor.html)
        this.setRejectedExecutionHandler(blockingExecutionHandler);
    }

    /**
     * Creates a thread pool that creates new threads as needed, but
     * will reuse previously constructed threads when they are
     * available.
     *
     * @param threadPoolName the name of the threads created by this executor
     * @return The new LogThreadPool
     */
    public static LogThreadPool createCachedThreadpoolWithMaxSize(String threadPoolName) {
        return new LogThreadPool(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedThreadFactory(threadPoolName));
    }

    /**
     * Returns a ThreadPoolExecutor with a fixed number of threads.
     * When all threads are actively executing tasks, new tasks are queued.
     * If (most) threads are expected to be idle most of the time, prefer createWithMaxSize() instead.
     *
     * @param threadPoolName the name of the threads created by this executor
     * @param size           the fixed number of threads for this executor
     * @return the new LogThreadPool
     */
    public static LogThreadPool createWithFixedPoolSize(String threadPoolName, int size) {
        return createWithMaximumPoolSize(threadPoolName, size, Integer.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Returns a ThreadPoolExecutor with a fixed maximum number of threads, but whose
     * threads are terminated when idle for too long.
     * When all threads are actively executing tasks, new tasks are queued.
     *
     * @param threadPoolName the name of the threads created by this executor
     * @param size           the maximum number of threads for this executor
     * @param keepAliveTime  the time an idle thread is kept alive before being terminated
     * @param unit           tht time unit for {@code keepAliveTime}
     * @return the new LogThreadPool
     */
    public static LogThreadPool createWithMaximumPoolSize(String threadPoolName, int size,
            int keepAliveTime, TimeUnit unit) {
        return new LogThreadPool(size, Integer.MAX_VALUE, keepAliveTime, unit,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory(threadPoolName));
    }

    protected void onInitialRejection(Runnable task) {
    }

    protected void onFinalAccept(Runnable task) {
    }

    protected void onFinalRejection(Runnable task) {
    }

    @Override
    public void maybeExecuteImmediately(Runnable command) {
        execute(command);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        logExceptionsAfterExecute(r, t);
    }

    /**
     * Send @param t and any exception wrapped by @param r to the default uncaught exception handler,
     * or log them if none such is set up
     */
    public static void logExceptionsAfterExecute(Runnable r, Throwable t) {
        Throwable hiddenThrowable = extractThrowable(r);
        if (hiddenThrowable != null) {
            handleOrLog(hiddenThrowable);
        }

        // ThreadPoolExecutor will re-throw exceptions thrown by its Task (which will be seen by
        // the default uncaught exception handler) so we only need to do anything if that handler
        // isn't set up yet.
        if (t != null && Thread.getDefaultUncaughtExceptionHandler() == null) {
            handleOrLog(t);
        }
    }

    /**
     * Send @param t to the default uncaught exception handler, or log it if none such is set up
     */
    public static void handleOrLog(Throwable t) {
        if (Thread.getDefaultUncaughtExceptionHandler() == null) {
            logger.error("Error in ThreadPoolExecutor", t);
        } else {
            Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
        }
    }

    /**
     * @return any exception wrapped by @param runnable, i.e., if it is a FutureTask
     */
    public static Throwable extractThrowable(Runnable runnable) {
        // Check for exceptions wrapped by FutureTask.  We do this by calling get(), which will
        // cause it to throw any saved exception.
        //
        // Complicating things, calling get() on a ScheduledFutureTask will block until the task
        // is cancelled.  Hence, the extra isDone check beforehand.
        if ((runnable instanceof Future<?>) && ((Future<?>) runnable).isDone()) {
            try {
                ((Future<?>) runnable).get();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } catch (CancellationException e) {
                logger.trace("Task cancelled", e);
            } catch (ExecutionException e) {
                return e.getCause();
            }
        }

        return null;
    }

}
