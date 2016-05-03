package com.revinate.henicea.util;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;

public class RetryerTest {

    @Test
    public void run_shouldReturnResultFromSupplier() throws Exception {
        String result = new Retryer()
                .run(() -> "unit test");

        assertThat(result).isEqualTo("unit test");
    }

    @Test
    public void run_shouldReturnOnFirstSuccess() throws Exception {
        List<Long> intervals = newArrayList();

        Supplier<String> errorSupplier = () -> {
            throw new RuntimeException();
        };

        Queue<Supplier<String>> suppliers = new LinkedList<>();
        suppliers.offer(errorSupplier);
        suppliers.offer(errorSupplier);
        suppliers.offer(() -> "success");

        String result = new Retryer()
                .withWait(TimeUnit.SECONDS, 1, 2)
                .usingWaitFunction(intervals::add)
                .run(() -> suppliers.poll().get());

        assertThat(result).isEqualTo("success");
        assertThat(intervals).containsExactly(0L, 1000L, 2000L);
    }

    @Test
    public void run_shouldRetryWithWaiting() throws Exception {
        List<Integer> retries = newArrayList();
        List<Long> intervals = newArrayList();

        assertThatThrownBy(() ->
                new Retryer()
                        .withWait(TimeUnit.SECONDS, 1, 2, 4)
                        .onError((i, ex) -> retries.add(i))
                        .usingWaitFunction(intervals::add)
                        .run(() -> {
                            throw new RuntimeException("unit test");
                        }))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("unit test");
        assertThat(retries).contains(1, 2, 3);
        assertThat(intervals).containsExactly(0L, 1000L, 2000L, 4000L);
    }
}