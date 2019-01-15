/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package apache.cassandra.concurrent;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

// done
public final class WaitQueue {

    private static final int CANCELLED = -1;
    private static final int SIGNALLED = 1;
    private static final int NOT_SET = 0;

    private static final AtomicIntegerFieldUpdater updater = AtomicIntegerFieldUpdater.newUpdater(
            RegisteredSignal.class, "state");

    // the waiting signals
    private final ConcurrentLinkedQueue<RegisteredSignal> queue = new ConcurrentLinkedQueue<>();

    protected WaitQueue() {

    }

    /**
     * The calling thread MUST be the thread that uses the signal
     */
    public Signal register() {
        RegisteredSignal signal = new RegisteredSignal();
        queue.add(signal);
        return signal;
    }

    /**
     * Signal one waiting thread
     */
    // 唤醒一个等待的信号量
    public boolean signal() {
        if (!hasWaiters()) {
            return false;
        }
        while (true) {
            RegisteredSignal s = queue.poll();
            if (null == s) {
                // 因为s != null，结果为假
                return false;
            } else if (null != s.signal()) {
                // 如果取出不为null，那么继续取
                // 如果同时唤醒成功(也就是返回值不为null)
                // 那么需要直接返回的
                return true;
            } else {
                // 如果取出但是没有唤醒成功
                // 那么继续
                continue;
            }
            // 这种逻辑比较难理解改造下
            // if (s == null || s.signal() != null) {
            // return s != null;
            // }
        }
    }

    /**
     * Signal all waiting threads
     */
    public void signalAll() {
        if (!hasWaiters()) {
            return;
        }

        // to avoid a race where the condition is not met and the woken thread managed to wait on the queue before
        // we finish signalling it all, we pick a random thread we have woken-up and hold onto it, so that if we
        // encounter
        // it again we know we're looping. We reselect a random thread periodically, progressively less often.
        // the "correct" solution to this problem is to use a queue that permits snapshot(快照) iteration, but this
        // solution is sufficient
        int i = 0, s = 5;
        Thread randomThread = null;
        Iterator<RegisteredSignal> iter = queue.iterator();
        while (iter.hasNext()) {
            RegisteredSignal signal = iter.next();
            if (null == signal) {
                continue;
            }
            Thread signalled = signal.signal();
            // 如果不为null，唤醒成功
            if (signalled != null) {
                // 如果这个随机的thread等于唤醒返回的thread
                // 说明之前的一个线程已经再次等待了
                // 所以这里竞争，那么此时可以返回了
                if (signalled == randomThread) {
                    break;
                }
                if (++i == s) {
                    randomThread = signalled;
                    s <<= 1;
                }
            }
            iter.remove();
        }
    }

    private void cleanUpCancelled() {
        // TODO: attempt to remove the cancelled from the beginning only (need atomic cas of head)
        // 为什么使用迭代器？为什么不使用queue的remove直接删除？
        Iterator<RegisteredSignal> iter = queue.iterator();
        while (iter.hasNext()) {
            RegisteredSignal s = iter.next();
            if (null != s && s.isCancelled()) {
                iter.remove();
            }
        }
    }

    public boolean hasWaiters() {
        return !queue.isEmpty();
    }

    /**
     * Return how many threads are waiting
     *
     * @return
     */
    public int getWaiting() {
        if (!hasWaiters()) {
            return 0;
        }
        Iterator<RegisteredSignal> iter = queue.iterator();
        int count = 0;
        while (iter.hasNext()) {
            Signal next = iter.next();
            if (!next.isCancelled()) {
                count++;
            }
        }
        return count;
    }

    /**
     * A Signal is a one-time-use mechanism for a thread to wait for notification that some condition
     * state has transitioned that it may be interested in (and hence should check if it is).
     * It is potentially transient, i.e. the state can change in the meantime, it only indicates
     * that it should be checked, not necessarily anything about what the expected state should be.
     * <p>
     * Signal implementations should never wake up spuriously, they are always woken up by a
     * signal() or signalAll().
     * <p>
     * This abstract definition of Signal does not need to be tied to a WaitQueue.
     * Whilst RegisteredSignal is the main building block of Signals, this abstract
     * definition allows us to compose Signals in useful ways. The Signal is 'owned' by the
     * thread that registered itself with WaitQueue(s) to obtain the underlying RegisteredSignal(s);
     * only the owning thread should use a Signal.
     */
    public static interface Signal {

        /**
         * @return true if signalled; once true, must be discarded by the owning thread.
         */
        boolean isSignalled();

        /**
         * @return true if cancelled; once cancelled, must be discarded by the owning thread.
         */
        boolean isCancelled();

        /**
         * @return isSignalled() || isCancelled(). Once true, the state is fixed and the Signal should be discarded
         * by the owning thread.
         */
        boolean isSet();

        /**
         * atomically: cancels the Signal if !isSet(), or returns true if isSignalled()
         *
         * @return true if isSignalled()
         */
        boolean checkAndClear();

        /**
         * Should only be called by the owning thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        void cancel();

        /**
         * Wait, without throwing InterruptedException, until signalled. On exit isSignalled() must be true.
         * If the thread is interrupted in the meantime, the interrupted flag will be set.
         */
        void awaitUninterruptibly();

        /**
         * Wait until signalled, or throw an InterruptedException if interrupted before this happens.
         * On normal exit isSignalled() must be true; however if InterruptedException is thrown isCancelled()
         * will be true.
         *
         * @throws InterruptedException
         */
        void await() throws InterruptedException;

        /**
         * Wait until signalled, or the provided time is reached, or the thread is interrupted. If signalled,
         * isSignalled() will be true on exit, and the method will return true; if timedout, the method will return
         * false and isCancelled() will be true; if interrupted an InterruptedException will be thrown and isCancelled()
         * will be true.
         *
         * @param nanos System.nanoTime() to wait until
         * @return true if signalled, false if timed out
         * @throws InterruptedException
         */
        boolean awaitUntil(long nanos) throws InterruptedException;
    }

    /**
     * An abstract signal implementation
     */
    public static abstract class AbstractSignal implements Signal {

        protected AbstractSignal() {

        }

        @Override
        public void awaitUninterruptibly() {
            boolean interrupted = false;
            while (!isSignalled()) {
                if (Thread.interrupted()) {
                    interrupted = true;
                }
                LockSupport.park();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            checkAndClear();
        }

        @Override
        public void await() throws InterruptedException {
            while (!isSignalled()) {
                checkInterrupted();
                LockSupport.park();
            }
            checkAndClear();
        }

        @Override
        public boolean awaitUntil(long until) throws InterruptedException {
            long now;
            while (until > (now = System.nanoTime()) && !isSignalled()) {
                checkInterrupted();
                long delta = until - now;
                LockSupport.parkNanos(delta);
            }
            return checkAndClear();
        }

        private void checkInterrupted() throws InterruptedException {
            if (Thread.interrupted()) {
                cancel();
                throw new InterruptedException();
            }
        }
    }

    /**
     * A signal registered with this WaitQueue
     */
    private class RegisteredSignal extends AbstractSignal {
        // 创建时候保存下创建当前对象的线程引用
        private volatile Thread thread = Thread.currentThread();
        volatile int state;

        // 是否已经被唤醒
        @Override
        public boolean isSignalled() {
            return state == SIGNALLED;
        }

        // 是否已经被取消
        @Override
        public boolean isCancelled() {
            return state == CANCELLED;
        }

        // 是否已经设置了一个值
        @Override
        public boolean isSet() {
            return state != NOT_SET;
        }

        private Thread signal() {
            // 如果没有设置状态并且cas成功
            // 那么唤醒创建这个信号的线程
            // 如果状态已经设置，那么直接返回null
            if (isSet()) {
                return null;
            } else if (updater.compareAndSet(this, NOT_SET, SIGNALLED)) {
                // 如果cas成功，则唤醒线程
                Thread thread = this.thread;
                LockSupport.unpark(thread);
                // 这个方法完成，说明此信号已经被废弃掉
                this.thread = null;
                // 返回创建当前对象的线程
                return thread;
            } else {
                return null;
            }
            // if (!isSet() && updater.compareAndSet(this, NOT_SET, SIGNALLED)) {
            // Thread thread = this.thread;
            // LockSupport.unpark(thread);
            // this.thread = null;
            // 返回创建当前对象的线程
            // return thread;
            // } else {
            //    return null;
            // }
        }

        @Override
        public boolean checkAndClear() {
            if (isSet()) {
                return true;
            } else if (updater.compareAndSet(this, NOT_SET, CANCELLED)) {
                thread = null;
                cleanUpCancelled();
                return false;
            } else {
                return true;
            }
            // 这两种写法的区别？？？
            // if (!isSet() && updater.compareAndSet(this, NOT_SET, CANCELLED)) {
            // thread = null;
            // cleanUpCancelled();
            // return false;
            // }
            // must now be signalled assuming correct API usage
            // return true;
        }

        /**
         * Should only be called by the registered thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        @Override
        public void cancel() {
            // 如果已经被取消，直接返回
            if (isCancelled()) {
                return;
            }
            // 如果设置失败，那么进行唤醒
            if (!setCancelState()) {
                setAndSignal();
            }
            release();
        }

        private boolean setCancelState() {
            if (updater.compareAndSet(this, NOT_SET, CANCELLED)) {
                return true;
            } else {
                return false;
            }
        }

        private void setAndSignal() {
            // must already be signalled - switch to cancelled and
            // 如果cas失败，说明状态已经被设置了(设置成取消或者唤醒)
            // 那么这时再设置为取消没有问题
            state = CANCELLED;
            // propagate the signal
            // 取消一个，唤醒一个
            WaitQueue.this.signal();
        }

        private void release() {
            // 线程已经被unpark
            thread = null;
            // 移除那些被取消的信号
            cleanUpCancelled();
        }

    }


    /**
     * An abstract signal wrapping multiple delegate signals
     */
    private abstract static class MultiSignal extends AbstractSignal {
        final Signal[] signals;

        protected MultiSignal(Signal[] signals) {
            this.signals = signals;
        }

        @Override
        public boolean isCancelled() {
            for (Signal signal : signals)
                if (!signal.isCancelled()) {
                    return false;
                }
            return true;
        }

        @Override
        public boolean checkAndClear() {
            for (Signal signal : signals)
                signal.checkAndClear();
            return isSignalled();
        }

        @Override
        public void cancel() {
            for (Signal signal : signals)
                signal.cancel();
        }
    }

    /**
     * A Signal that wraps multiple Signals and returns when any single one of them would have returned
     */
    private static class AnySignal extends MultiSignal {

        protected AnySignal(Signal... signals) {
            super(signals);
        }

        @Override
        public boolean isSignalled() {
            for (Signal signal : signals)
                if (signal.isSignalled()) {
                    return true;
                }
            return false;
        }

        @Override
        public boolean isSet() {
            for (Signal signal : signals)
                if (signal.isSet()) {
                    return true;
                }
            return false;
        }
    }

    /**
     * A Signal that wraps multiple Signals and returns when all of them would have finished returning
     */
    private static class AllSignal extends MultiSignal {
        protected AllSignal(Signal... signals) {
            super(signals);
        }

        @Override
        public boolean isSignalled() {
            for (Signal signal : signals)
                if (!signal.isSignalled()) {
                    return false;
                }
            return true;
        }

        @Override
        public boolean isSet() {
            for (Signal signal : signals)
                if (!signal.isSet()) {
                    return false;
                }
            return true;
        }
    }

    /**
     * @param signals
     * @return a signal that returns only when any of the provided signals would have returned
     */
    public static Signal any(Signal... signals) {
        return new AnySignal(signals);
    }

    /**
     * @param signals
     * @return a signal that returns only when all provided signals would have returned
     */
    public static Signal all(Signal... signals) {
        return new AllSignal(signals);
    }
}
