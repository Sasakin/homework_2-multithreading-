package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter {
    private static final Logger logger = getLogger(FileWriter.class);

    public FileWriter() {
    }

    public void write(List<Pair<String, Integer>> futureList, BufferedOutputStream os) {
        if(futureList == null) {
            return;
        }

        logger.info("Started writer thread {}", currentThread().getName());

        try {

            futureList.forEach(future -> {
                try {
                    String s = future.getKey() + " " + future.getValue() + "\n";
                    os.write(s.getBytes());
                } catch (IOException e) {
                    logger.info("Finish writer thread {}", e.toString());
                }
            });

        } catch (Exception ioe) {
            logger.info("Finish writer thread {}", ioe.toString());
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }

}
