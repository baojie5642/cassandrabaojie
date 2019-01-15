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

        BaojieExecutorService service = share.newExecutor(512, 1024, "test");
        TestRunner runner= null;
        for(;;){
            runner=new TestRunner();
            service.submit(runner);
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(300, TimeUnit.MICROSECONDS));
        }

    }


    public static final class TestRunner implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

        @Override
        public void run() {
            //logger.debug("park myself");
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS));
            //logger.debug("unpark myself");
        }

    }


}
