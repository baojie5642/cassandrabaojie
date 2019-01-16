package apache.cassandra.test;

import apache.cassandra.concurrent.BaojieExecutorService;
import apache.cassandra.concurrent.SharedExecutorPool;

public class CassandraStage {

    public static void main(String args[]) {
        SharedExecutorPool share = new SharedExecutorPool("test-name");
        BaojieExecutorService service = share.newExecutor(555, 555, "test");
        SubmitRunner runner = null;
        for (int i = 0; i < 1; i++) {
            runner = new SubmitRunner();
            // 测试结果是，这里有些小问题，就是提交后马上关闭pool会出现丢任务情况
            service.submit(runner);
            //
        }
        //LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(9, TimeUnit.SECONDS));
        service.shutdown();
        System.out.println("main has shutdown");
    }

}
