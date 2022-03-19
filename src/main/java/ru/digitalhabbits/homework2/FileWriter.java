package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    private List<Pair<String, Integer>> futureList;

    private Semaphore semaphore;

    private String resultFile;

    public FileWriter(String resultFile, Semaphore semaphore) {
        this.resultFile = resultFile;
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        if(futureList == null) {
            return;
        }

        logger.info("Started writer thread {}", currentThread().getName());

        DataOutputStream os = null;

        try {
            os = new DataOutputStream(
                    new FileOutputStream(resultFile, true));

            DataOutputStream finalOs = os;
            futureList.forEach(future -> {
                try {
                    finalOs.writeBytes(future.getKey() + " " + future.getValue() + "\n");
                } catch (IOException e) {
                    logger.info("Finish writer thread {}", e.toString());
                }
            });

            finalOs.flush();
            finalOs.close();

        } catch (Exception ioe) {
            logger.info("Finish writer thread {}", ioe.toString());
        } finally {
            futureList = null;
            semaphore.release();
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }

    public void setFuture(List<Pair<String, Integer>> futureList) {
        this.futureList = futureList;
    }
}
