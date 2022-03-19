package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

public class LineProcessorThread implements Callable<Pair<String, Integer>> {

    private final String line;

    private final Semaphore semaphore;

    public LineProcessorThread(String line, Semaphore semaphore) {
        this.line = line;
        this.semaphore = semaphore;
    }

    @Override
    public Pair<String, Integer> call() throws Exception {
        Pair pair = new LineCounterProcessor().process(line);
        semaphore.release(1);
        return  pair;
    }
}
