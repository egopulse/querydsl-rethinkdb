package com.egopulse.querydsl.rethinkdb.core;

import rx.Single;

import java.util.List;

public interface RxFetchable<T> {

    Single<List<T>> fetch();

    Single<T> fetchFirst();

    Single<T> fetchOne();

    Single<Long> fetchCount();

}
