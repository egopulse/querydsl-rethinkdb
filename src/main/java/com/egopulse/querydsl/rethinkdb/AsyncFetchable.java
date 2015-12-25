package com.egopulse.querydsl.rethinkdb;

import rx.Single;

import java.util.List;

public interface AsyncFetchable<T> {

    Single<List<T>> fetch();

    Single<T> fetchFirst();

    Single<T> fetchOne();

    Single<Long> fetchCount();

}
