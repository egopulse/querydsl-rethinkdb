package com.egopulse.querydsl.rethinkdb;

import com.rethinkdb.net.Connection;
import rx.Single;

public interface ReturnableConnection {

    void handBack();

    Single<Connection> getConnection();

}
