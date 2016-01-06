package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.helper.DummyReturnableConnection;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    @SuppressWarnings("unchecked")
    public static Map<String, ?> fetchFirst(Cursor<Object> cursor) {
        if (!(cursor.hasNext())) {
            throw new AssertionError("Empty result set");
        }
        return (Map<String, ?>) cursor.next();
    }

    @SuppressWarnings("unchecked")
    public static Collection<Map<String, ?>> fetch(Cursor<Object> cursor) {
        List<Map<String, ?>> ret = new ArrayList<>();
        for (Object o : cursor) {
            ret.add((Map<String, ?>) o);
        }
        return ret;
    }

}
