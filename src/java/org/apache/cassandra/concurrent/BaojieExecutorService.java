package apache.cassandra.concurrent;

import java.util.concurrent.ExecutorService;

public interface BaojieExecutorService extends ExecutorService {

    // permits executing in the context of the submitting thread
    // 也就是允许当前线程执行，允许当前提交线程执行
    void maybeExecuteImmediately(Runnable command);
}
