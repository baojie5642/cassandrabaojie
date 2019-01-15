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

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

// fulfils the Condition interface without spurious wakeup problems
// (or lost notify problems either: that is, even if you call await()
// _after_ signal(), it will work as desired.)

// done
public class SimpleCondition implements Condition {
    // 这种方式经常用来构建lockfree并发工具
    private static final AtomicReferenceFieldUpdater<SimpleCondition, WaitQueue> updater =
            AtomicReferenceFieldUpdater.newUpdater(SimpleCondition.class, WaitQueue.class, "waiting");

    private volatile WaitQueue waiting;
    private volatile boolean signaled = false;

    protected SimpleCondition() {

    }

    @Override
    public void await() throws InterruptedException {
        // 已经被唤醒，那么直接返回
        if (isSignaled()) {
            return;
        }
        // 如果队列为null，那么cas一个
        // 虽然可能会创建多余的new对象
        // 但是不重要，不必强行同步
        if (waiting == null) {
            updater.compareAndSet(this, null, new WaitQueue());
        }
        // 将自己注册到队列上，同时获得一个信号
        WaitQueue.Signal s = waiting.register();
        // 如果被唤醒，那么取消
        if (isSignaled()) {
            s.cancel();
        } else {
            // 如果没有被唤醒，那么一直等待
            /**
             *   TODO 这里可能需要修改
             **/
            s.await();
        }
        // 正常结束，那么断言
        assert isSignaled();
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        if (isSignaled()) {
            return true;
        }
        long start = System.nanoTime();
        long until = start + unit.toNanos(time);
        if (waiting == null) {
            updater.compareAndSet(this, null, new WaitQueue());
        }
        WaitQueue.Signal s = waiting.register();
        if (isSignaled()) {
            s.cancel();
            return true;
        }
        return s.awaitUntil(until) || isSignaled();
    }

    @Override
    public void signal() {
        throw new UnsupportedOperationException();
    }

    public boolean isSignaled() {
        return signaled;
    }

    @Override
    public void signalAll() {
        signaled = true;
        if (waiting != null) {
            waiting.signalAll();
        }
    }

    @Override
    public void awaitUninterruptibly() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long awaitNanos(long nanosTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitUntil(Date deadline) {
        throw new UnsupportedOperationException();
    }
}
