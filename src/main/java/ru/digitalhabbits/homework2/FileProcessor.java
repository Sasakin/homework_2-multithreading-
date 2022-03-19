package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    Semaphore semaphore = new Semaphore(-CHUNK_SIZE + 1);

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        long millisec = System.currentTimeMillis();

        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        FileWriter fileWriter = new FileWriter(resultFileName, semaphore);
        Thread writerThread = new Thread(fileWriter);

        ExecutorService executor = Executors.newFixedThreadPool(CHUNK_SIZE);
        List<Callable<Pair<String, Integer>>> threadList = new ArrayList<>();
        List<Future<Pair<String, Integer>>> futureList = new ArrayList<>();
        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {

                while (threadList.size() < CHUNK_SIZE && scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    Callable<Pair<String, Integer>> lineThread = new LineProcessorThread(line, semaphore);
                    threadList.add(lineThread);
                }

                threadList.forEach(t -> {
                    try {
                        futureList.add(executor.submit(t));
                    } catch (Throwable e) {
                        logger.error("", e);
                    }
                });

                semaphore.acquire(1);

                fileWriter.setFuture(futureList.stream().map(f -> {
                    try {
                        return f.get();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList()));


                executor.submit(writerThread);

                semaphore.acquire(1);

                threadList.clear();
                futureList.clear();

            }
        } catch (Throwable exception) {
            logger.error("", exception);
        } finally {
            executor.shutdown();
            logger.info("Потоки закончили работу");
        }

        logger.info("Finish main thread {}", Thread.currentThread().getName());
        logger.info("Process time: {}", System.currentTimeMillis() - millisec);
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
