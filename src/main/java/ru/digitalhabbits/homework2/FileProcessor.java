package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        long millisec = System.currentTimeMillis();

        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        FileWriter fileWriter = new FileWriter();

        ExecutorService executor = Executors.newFixedThreadPool(CHUNK_SIZE);
        List<Future<Pair<String, Integer>>> futureList = new ArrayList<>();
        try (final Scanner scanner = new Scanner(file, defaultCharset());
             final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                     new FileOutputStream(resultFileName, true))) {
            logger.info("Перед while");
            while (scanner.hasNext()) {
                logger.info("После while");
                String line = scanner.nextLine();

                Callable<Pair<String, Integer>> lineThread = new LineProcessorThread(line);
                futureList.add(executor.submit(lineThread));
            }

            logger.info("Перед await");
            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);

            logger.info("После await");

            List<Pair<String, Integer>> pairsList = futureList.stream().map(f -> {
                try {
                    return f.get();
                } catch (Throwable e) {
                    logger.error("Ошибка расчета: ", e);
                    throw new RuntimeException("Ошибка расчета", e);
                }
            }).collect(Collectors.toList());

            fileWriter.write(pairsList, bufferedOutputStream);

            futureList.clear();

        } catch (Throwable exception) {
            logger.error("", exception);
        } finally {
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
