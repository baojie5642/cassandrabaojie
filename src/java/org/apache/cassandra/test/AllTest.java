package apache.cassandra.test;


import apache.cassandra.concurrent.BaojieExecutorService;
import apache.cassandra.concurrent.SharedExecutorPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class AllTest {

    public static void main(String args[]) {
        SharedExecutorPool share = new SharedExecutorPool("test-name");

        BaojieExecutorService service = share.newExecutor(1, 1, "test", "test_name");
        TestRunner runner0 = new TestRunner();
        service.submit(runner0);

        TestRunner runner1 = new TestRunner();
        service.submit(runner1);


    }


    public static final class TestRunner implements Runnable {


        @Override
        public void run() {
            System.out.println("park myself");
            LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(3600, TimeUnit.SECONDS));
            System.out.println("unpark myself");
        }

    }


}
