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

import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Like LogThreadPool, LogScheduledPool always
 * logs exceptions from the tasks it is given, even if Future.get is never called elsewhere.
 * <p>
 * LogScheduledPool also catches exceptions during Task execution
 * so that they don't supress subsequent invocations of the task.
 */
// 一个无论什么情况发生都会答印异常的定时任务线程池
public class LogScheduledPool extends ScheduledThreadPoolExecutor {
    private static final Logger logger = LoggerFactory.getLogger(LogScheduledPool.class);

    public LogScheduledPool(int corePoolSize, String threadPoolName, int priority) {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
    }


    public LogScheduledPool(String threadPoolName) {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
    }

    // We need this as well as the wrapper for the benefit of non-repeating tasks
    @Override
    public void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        LogThreadPool.logExceptionsAfterExecute(r, t);
    }

    // override scheduling to supress exceptions that would cancel future executions
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(new UncomplainingRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(new UncomplainingRunnable(command), initialDelay, delay, unit);
    }

    private static class UncomplainingRunnable implements Runnable {
        private final Runnable runnable;

        public UncomplainingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                //JVMStabilityInspector.inspectThrowable(t);
                LogThreadPool.handleOrLog(t);
            }
        }
    }
}