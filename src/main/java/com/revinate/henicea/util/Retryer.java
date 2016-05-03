package com.revinate.henicea.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayList;

@Slf4j
public class Retryer {

    private List<Long> waits = newArrayList(0L);
    private BiConsumer<Integer, Throwable> notifier = (i, t) -> {
    };

    private Consumer<Long> waitFunction = interval -> {
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            log.warn("Sleep was interrupted", e);
        }
    };

    @VisibleForTesting
    Retryer usingWaitFunction(Consumer<Long> waitFunction) {
        this.waitFunction = waitFunction;
        return this;
    }

    public Retryer withWait(TimeUnit timeUnit, Integer... waitTime) {
        Stream.of(waitTime)
                .mapToLong(timeUnit::toMillis)
                .forEach(waits::add);
        return this;
    }

    public Retryer onError(BiConsumer<Integer, Throwable> onError) {
        notifier = onError;
        return this;
    }

    public <T> T run(Supplier<T> supplier) {
        RuntimeException lastException = null;
        for (int i = 0; i < waits.size(); i++) {
            waitFunction.accept(waits.get(i));

            try {
                return supplier.get();
            } catch (RuntimeException t) {
                notifier.accept(i + 1, t);
                lastException = t;
            }
        }
        throw lastException;
    }
}
