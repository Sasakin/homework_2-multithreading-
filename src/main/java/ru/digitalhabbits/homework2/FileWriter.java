package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    private Pair<String, Integer> future;

    private Lock lock = new ReentrantLock();

    private Condition isEmpty = lock.newCondition();

    private Condition isNotEmpty = lock.newCondition();

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
                lock.lock();
                if (future != null) {
                    os.writeBytes(future.getKey() + " " + future.getValue() + "\n");
                    future = null;
                    isNotEmpty.signalAll();
                } else {
                    isEmpty.await();
                }
                lock.unlock();
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
        lock.lock();
        isEmpty.signal();
        if (this.future != null) {
            try {
                isNotEmpty.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.future = future;
        lock.unlock();
    }

    public void shutdown() {
        lock.lock();
        isEmpty.signalAll();
        shutdown = true;
        lock.unlock();
    }
}
