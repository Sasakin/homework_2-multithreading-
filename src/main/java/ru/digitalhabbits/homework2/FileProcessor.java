package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        FileWriter fileWriter = new FileWriter(resultFileName);
        Thread writerThread = new Thread(fileWriter);
        writerThread.start();

        ExecutorService executor = Executors.newFixedThreadPool(CHUNK_SIZE);
        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {

                Stream<Callable<Pair<String, Integer>>> threadStream = Stream.generate(() -> {
                    if (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        Callable<Pair<String, Integer>> lineThread = new LineProcessorThread(line);
                        return lineThread;
                    }
                    return null; // TODO fixmy
                });

                threadStream.limit(CHUNK_SIZE).filter(t -> t!=null).forEach(t -> {
                    try {
                        fileWriter.setFuture(executor.submit(t).get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                });

            }
        } catch (IOException exception) {
            logger.error("", exception);
            executor.shutdown();
            fileWriter.shutdown();
        } finally {
            executor.shutdown();
            fileWriter.shutdown();
            logger.info("Потоки закончили работу");
        }

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
