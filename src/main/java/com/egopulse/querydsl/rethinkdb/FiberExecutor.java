package com.egopulse.querydsl.rethinkdb;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import rx.Single;
import rx.subjects.AsyncSubject;

import java.util.function.Supplier;

/**
 * Currently a static interface for convenient execute blocking tasks
 * Should turn to instance interface if needed for custom scheduler or similar
 */
public class FiberExecutor {

    public static <T> Single<T> execute(Supplier<T> supplier) {
        AsyncSubject<T> subject = AsyncSubject.create();

        Fiber fiber = new Fiber<Void>() {
            @Override
            protected Void run() throws SuspendExecution, InterruptedException {
                subject.onNext(supplier.get());
                subject.onCompleted();
                return null;
            }
        };
        fiber.setUncaughtExceptionHandler((f, e) -> subject.onError(e));
        fiber.start();

        return subject.toSingle();
    }

}
