package apache.cassandra.test;

import apache.cassandra.concurrent.BaojieExecutorService;
import apache.cassandra.concurrent.SharedExecutorPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class AllTest {

    public static void main(String args[]) {
        SharedExecutorPool share = new SharedExecutorPool("test-name");
        BaojieExecutorService service = share.newExecutor(555, 555, "test");
        TestRunner runner = null;
        for (int i = 0; i < 1; i++) {
            runner = new TestRunner();
            // 测试结果是，这里有些小问题，就是提交后马上关闭pool会出现丢任务情况
            service.submit(runner);
            //
        }
        //LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(9, TimeUnit.SECONDS));
        service.shutdown();
    }

    public static final class TestRunner implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

        @Override
        public void run() {
            logger.debug("park myself");
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(6, TimeUnit.SECONDS));
            logger.debug("unpark myself");
            throw new OutOfMemoryError("test");
        }
    }

}
