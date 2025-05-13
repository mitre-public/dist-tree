// package org.mitre.oldtree.core;
//
// import java.util.concurrent.ExecutionException;
// import java.util.concurrent.Future;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.TimeoutException;
//
/// **
// * A TrivialFuture is the simplest possible implementation of Future.  It is useful when results are
// * immediately available, but we still need to return a Future<T> to match an interface signature.
// *
// * This adapter allows a fast In-Memory process to "look like" an asynchronous process.
// */
// public record TrivialFuture<T>(T payload) implements Future<T> {
//
//    /** Convert a T into a Future<T>. */
//    public static <T> TrivialFuture<T> asFuture(T item) {
//        return new TrivialFuture<>(item);
//    }
//
//    @Override
//    public boolean cancel(boolean mayInterruptIfRunning) {
//        return false;
//    }
//
//    @Override
//    public boolean isCancelled() {
//        return false;
//    }
//
//    @Override
//    public boolean isDone() {
//        return true;
//    }
//
//    @Override
//    public T get() throws InterruptedException, ExecutionException {
//        return payload;
//    }
//
//    @Override
//    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
//        return payload;
//    }
// }
