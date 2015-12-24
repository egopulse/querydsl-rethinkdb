package com.egopulse.querydsl.rethinkdb;

import com.rethinkdb.net.Connection;

public class ReturnableConnection {
    private Connection connection;

    // TODO: testing purpose only,
    // the ReturnableConnection should hold a reference to a pool to return the connection later
    public ReturnableConnection(Connection connection) {
        this.connection = connection;
    }

    public void returnToPool() {
        System.out.println("Supposed to be returned to the pool");
    }

    public Connection getConnection() {
        return connection;
    }

}
