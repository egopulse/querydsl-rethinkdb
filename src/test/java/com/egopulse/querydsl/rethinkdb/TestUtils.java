package com.egopulse.querydsl.rethinkdb;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class TestUtils {
    public static void withConnection(Consumer<Connection> dbOperation) throws TimeoutException {
        Connection connection = RethinkDB.r.connection().connect();
        dbOperation.accept(connection);
        connection.close();
    }


}
