package apache.cassandra.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class SubmitRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SubmitRunner.class);

    public SubmitRunner() {

    }

    @Override
    public void run() {
        final String tn=Thread.currentThread().getName();
        logger.debug("park myself");
        LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(6, TimeUnit.SECONDS));
        logger.debug("unpark myself");
        throw new OutOfMemoryError("i am '"+tn+"' throw an error");
    }

}
