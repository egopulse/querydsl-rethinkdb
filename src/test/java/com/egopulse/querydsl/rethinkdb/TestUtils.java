package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.helper.DummyReturnableConnection;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class TestUtils {

    public static Connection sharedConnection;
    static {
        try {
            sharedConnection = RethinkDB.r.connection().connect();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static void withConnection(Consumer<Connection> dbOperation) throws TimeoutException {
        dbOperation.accept(sharedConnection);
    }

    public synchronized static void withReturnableConnection(Consumer<DummyReturnableConnection> dbOperation) throws TimeoutException {
        dbOperation.accept(new DummyReturnableConnection(sharedConnection));
    }

}
