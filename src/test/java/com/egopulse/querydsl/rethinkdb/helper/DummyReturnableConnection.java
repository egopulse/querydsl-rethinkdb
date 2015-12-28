package com.egopulse.querydsl.rethinkdb.helper;

import com.egopulse.querydsl.rethinkdb.ReturnableConnection;
import com.rethinkdb.net.Connection;
import rx.Single;

/**
 * This class is for the testing sake only
 */
public class DummyReturnableConnection implements ReturnableConnection {
    private Single<Connection> connection;

    public DummyReturnableConnection(Connection connection) {
        this.connection = Single.just(connection);
    }

    @Override
    public void handBack() {
        // Do nothing
    }

    @Override
    public Single<Connection> getConnection() {
        return connection;
    }

}
