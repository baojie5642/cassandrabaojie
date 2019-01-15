package apache.cassandra.concurrent;

import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HeapUtils {

    private static final Logger log = LoggerFactory.getLogger(HeapUtils.class);

    public static void generateHeapDump() {
        Long processId = getProcessId();
        if (processId == null) {
            log.error("The process ID could not be retrieved. Skipping heap dump generation.");
            return;
        }
        String heapDumpPath = getHeapDumpPathOption();
        if (heapDumpPath == null) {
            String javaHome = System.getenv("JAVA_HOME");
            if (javaHome == null) {
                return;
            }
            heapDumpPath = javaHome;
        }
        Path dumpPath = FileSystems.getDefault().getPath(heapDumpPath);
        if (Files.isDirectory(dumpPath)) {
            dumpPath = dumpPath.resolve("java_pid" + processId + ".hprof");
        }
        String jmapPath = getJmapPath();
        // The jmap file could not be found. In this case let's default to jmap in the hope that it is in the path.
        String jmapCommand = jmapPath == null ? "jmap" : jmapPath;
        String[] dumpCommands = new String[]{jmapCommand,
                "-dump:format=b,file=" + dumpPath,
                processId.toString()};

        // Lets also log the Heap histogram
        String[] histoCommands = new String[]{jmapCommand,
                "-histo",
                processId.toString()};
        try {
            logProcessOutput(Runtime.getRuntime().exec(dumpCommands));
            logProcessOutput(Runtime.getRuntime().exec(histoCommands));
        } catch (IOException e) {
            log.error("The heap dump could not be generated due to the following error: ", e);
        }
    }

    /**
     * Retrieve the path to the JMAP executable.
     *
     * @return the path to the JMAP executable or null if it cannot be found.
     */
    private static String getJmapPath() {
        // Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
        // JVM might use the JRE which do not contains jmap.
        String javaHome = System.getenv("JAVA_HOME");
        if (javaHome == null) {
            return null;
        }

        File javaBinDirectory = new File(javaHome, "bin");
        File[] files = javaBinDirectory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("jmap");
            }
        });
        return ArrayUtils.isEmpty(files) ? null : files[0].getPath();
    }

    /**
     * Logs the output of the specified process.
     *
     * @param p the process
     * @throws IOException if an I/O problem occurs
     */
    private static void logProcessOutput(Process p) throws IOException {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            StrBuilder builder = new StrBuilder();
            String line;
            while ((line = input.readLine()) != null) {
                builder.appendln(line);
            }
            log.info(builder.toString());
        }
    }

    /**
     * Retrieves the value of the <code>HeapDumpPath</code> JVM option.
     *
     * @return the value of the <code>HeapDumpPath</code> JVM option or <code>null</code> if the value has not been
     * specified.
     */

    private static final String OP_KEY = "-XX:HeapDumpPath=";

    private static String getHeapDumpPathOption() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArguments = runtimeMxBean.getInputArguments();
        String heapDumpPathOption = null;
        for (String argument : inputArguments) {
            if (argument.startsWith(OP_KEY)) {
                heapDumpPathOption = argument;
                // We do not break in case the option has been specified several times.
                // In general it seems that JVMs use the right-most argument as the winner.
            }
        }
        if (heapDumpPathOption == null) {
            return null;
        } else {
            int len = heapDumpPathOption.length();
            return heapDumpPathOption.substring(OP_KEY.length(), len);
        }

    }

    private static Long getProcessId() {
        return getProcessIdFromJvmName();
    }

    private static Long getProcessIdFromJvmName() {
        // the JVM name in Oracle JVMs is: '<pid>@<hostname>' but this might not be the case on all JVMs
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        if (null == jvmName) {
            log.error("get jvm name null;");
            return null;
        }
        try {
            return Long.valueOf(jvmName.split("@")[0]);
        } catch (Throwable t) {
            log.error(t.toString() + ", jvm name=" + jvmName, t);
        }
        return null;
    }

    private HeapUtils() {
    }

}
