package uj.wmii.pwj.exec;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyExecService implements ExecutorService {
    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
    private final Thread worker;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);

    public static MyExecService newInstance() {
        return new MyExecService();
    }

    public MyExecService() {
        worker = new Thread(() -> {
            try {
                while (true) {
                    Runnable task;
                    if (isShutdown.get() && taskQueue.isEmpty()) break;
                    task = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        try {
                            task.run();
                        } catch (Throwable ignored) {}
                    }
                }
            } catch (InterruptedException e) {
                // Exit worker thread
            } finally {
                isTerminated.set(true);
            }
        });
        worker.start();
    }

    @Override
    public void shutdown() {
        isShutdown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        isShutdown.set(true);
        worker.interrupt();
        List<Runnable> notExecuted = new ArrayList<>();
        taskQueue.drainTo(notExecuted);
        return notExecuted;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return isTerminated.get();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        worker.join(unit.toMillis(timeout));
        return isTerminated();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (isShutdown.get()) throw new RejectedExecutionException();
        CompletableFuture<T> future = new CompletableFuture<>();
        taskQueue.offer(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (isShutdown.get()) throw new RejectedExecutionException();
        CompletableFuture<T> future = new CompletableFuture<>();
        taskQueue.offer(() -> {
            try {
                task.run();
                future.complete(result);
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    @Override
    public Future<?> submit(Runnable task) {
        if (isShutdown.get()) throw new RejectedExecutionException();
        CompletableFuture<Void> future = new CompletableFuture<>();
        taskQueue.offer(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (Throwable ex) {
                future.completeExceptionally(ex);
            }
        });
        return future;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        for (Future<T> future : futures) {
            try {
                future.get();
            } catch (ExecutionException ignored) {}
        }
        return futures;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        List<Future<T>> futures = new ArrayList<>();
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        for (Future<T> future : futures) {
            long timeLeft = deadline - System.nanoTime();
            if (timeLeft <= 0) break;
            try {
                future.get(timeLeft, TimeUnit.NANOSECONDS);
            } catch (ExecutionException | TimeoutException ignored) {
            }
        }
        for (Future<T> future : futures) {
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        List<Future<T>> futures = new ArrayList<>();
        try {
            for (Callable<T> task : tasks) {
                futures.add(submit(task));
            }
            for (Future<T> future : futures) {
                try {
                    return future.get();
                } catch (ExecutionException ignored) {}
            }
            throw new ExecutionException(new Exception("No task completed successfully"));
        } finally {
            for (Future<T> f : futures) f.cancel(true);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Future<T>> futures = new ArrayList<>();
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        try {
            for (Callable<T> task : tasks) {
                futures.add(submit(task));
            }
            for (Future<T> future : futures) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) break;
                try {
                    return future.get(timeLeft, TimeUnit.NANOSECONDS);
                } catch (ExecutionException ignored) {}
            }
            throw new TimeoutException("invokeAny timeout or no task completed successfully");
        } finally {
            for (Future<T> f : futures) f.cancel(true);
        }
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown.get()) throw new RejectedExecutionException();
        taskQueue.offer(command);
    }
}