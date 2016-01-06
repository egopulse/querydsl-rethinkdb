package com.egopulse.querydsl.rethinkdb.core;

import rx.Single;

import java.util.Map;

public interface RxDMLClause<C> {

    Single<Map<String, Long>> execute();

}
