package apache.cassandra.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.SocketException;

public class JVMStabilityInspector {
    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    private static final Killer killer = new Killer();

    private JVMStabilityInspector() {
    }

    /**
     * Certain Throwables and Exceptions represent "Die" conditions for the server.
     *
     * @param t The Throwable to check for server-stop conditions
     */
    public static void inspectThrowable(Throwable t) {
        boolean isUnstable = false;
        if (t instanceof OutOfMemoryError) {
            isUnstable = true;
            HeapUtils.generateHeapDump();
        }
        // Check for file handle exhaustion
        else if (t instanceof FileNotFoundException || t instanceof SocketException) {
            if (t.getMessage().contains("Too many open files")) {
                isUnstable = true;
            }
        }

        if (isUnstable) {
            killer.killCurrentJVM(t);
        }
    }

    // @VisibleForTesting
    public static class Killer {
        /**
         * Certain situations represent "Die" conditions for the server, and if so,
         * the reason is logged and the current JVM is killed.
         *
         * @param t The Throwable to log before killing the current JVM
         */
        protected void killCurrentJVM(Throwable t) {
            t.printStackTrace(System.err);
            logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
            ShutdownHookUtils.removeAllShutdownHooks();
            System.exit(100);
        }
    }
}
