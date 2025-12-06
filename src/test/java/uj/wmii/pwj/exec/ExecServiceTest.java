package uj.wmii.pwj.exec;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

public class ExecServiceTest {

    @Test
    void testExecute() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.execute(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnable() {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        s.submit(r);
        doSleep(10);
        assertTrue(r.wasRun);
    }

    @Test
    void testScheduleRunnableWithResult() throws Exception {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r = new TestRunnable();
        Object expected = new Object();
        Future<Object> f = s.submit(r, expected);
        doSleep(10);
        assertTrue(r.wasRun);
        assertTrue(f.isDone());
        assertEquals(expected, f.get());
    }

    @Test
    void testScheduleCallable() throws Exception {
        MyExecService s = MyExecService.newInstance();
        StringCallable c = new StringCallable("X", 10);
        Future<String> f = s.submit(c);
        doSleep(20);
        assertTrue(f.isDone());
        assertEquals("X", f.get());
    }

    @Test
    void testShutdown() {
        ExecutorService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        doSleep(10);
        s.shutdown();
        assertThrows(
            RejectedExecutionException.class,
            () -> s.submit(new TestRunnable()));
    }

    static void doSleep(int milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    void testShutdownNow() throws Exception {
        MyExecService s = MyExecService.newInstance();
        TestRunnable r1 = new TestRunnable();
        s.execute(r1);
        List<Runnable> remaining = s.shutdownNow();
        assertTrue(s.isShutdown());
        assertNotNull(remaining);
    }

    @Test
    void testIsTerminatedAfterShutdown() throws Exception {
        MyExecService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        s.shutdown();
        // Czekamy chwilę na zakończenie
        for (int i = 0; i < 50 && !s.isTerminated(); i++) ExecServiceTest.doSleep(10);
        assertTrue(s.isTerminated());
    }

    @Test
    void testAwaitTermination() throws InterruptedException {
        MyExecService s = MyExecService.newInstance();
        s.execute(new TestRunnable());
        s.shutdown();
        assertTrue(s.awaitTermination(200, TimeUnit.MILLISECONDS));
    }

    @Test
    void testInvokeAll() throws Exception {
        MyExecService s = MyExecService.newInstance();
        Callable<String> c1 = () -> "A";
        Callable<String> c2 = () -> "B";
        List<Future<String>> futures = s.invokeAll(Arrays.asList(c1, c2));
        assertEquals("A", futures.get(0).get());
        assertEquals("B", futures.get(1).get());
    }

    @Test
    void testInvokeAny() throws Exception {
        MyExecService s = MyExecService.newInstance();
        Callable<String> c1 = () -> "Z";
        String result = s.invokeAny(Collections.singletonList(c1));
        assertEquals("Z", result);
    }

    @Test
    void testInvokeAllTimeout() throws Exception {
        MyExecService s = MyExecService.newInstance();
        Callable<String> c1 = () -> {
            ExecServiceTest.doSleep(200);
            return "A";
        };
        List<Future<String>> futures = s.invokeAll(Collections.singletonList(c1), 100, TimeUnit.MILLISECONDS);
        assertTrue(futures.get(0).isDone());
    }

    @Test
    void testInvokeAnyTimeout() {
        MyExecService s = MyExecService.newInstance();
        Callable<String> c1 = () -> {
            ExecServiceTest.doSleep(200);
            return "A";
        };
        assertThrows(TimeoutException.class, () -> {
            s.invokeAny(Collections.singletonList(c1), 50, TimeUnit.MILLISECONDS);
        });
    }

}

class StringCallable implements Callable<String> {

    private final String result;
    private final int milis;

    StringCallable(String result, int milis) {
        this.result = result;
        this.milis = milis;
    }

    @Override
    public String call() throws Exception {
        ExecServiceTest.doSleep(milis);
        return result;
    }


}
class TestRunnable implements Runnable {

    boolean wasRun;
    @Override
    public void run() {
        wasRun = true;
    }
}


