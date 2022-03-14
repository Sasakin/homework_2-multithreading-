package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    private Pair<String, Integer> future;

    //Phaser phaser = new Phaser(1);

    private boolean shutdown = false;

    private String resultFile;

    public FileWriter(String resultFile) {
        this.resultFile = resultFile;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        DataOutputStream os;

        try
        {
            os = new DataOutputStream(
                    new FileOutputStream(resultFile, true));

            while (!shutdown) {
                synchronized (this) {
                    if (future != null) {
                        os.writeBytes(future.getKey() + " " + future.getValue() + "\n");
                        future = null;
                        this.notifyAll();
                    }
                }
            }

            os.flush();
            os.close();
        }
        catch(Exception ioe)
        {
            logger.info("Finish writer thread {}", ioe.toString());
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }

    public void setFuture(Pair<String, Integer> future) {
        synchronized (this) {
            if(this.future != null) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.future = future;
        }
    }

    private void shutdown() {
        shutdown = true;
    }
}
