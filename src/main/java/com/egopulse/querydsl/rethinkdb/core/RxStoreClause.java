package com.egopulse.querydsl.rethinkdb.core;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;

import javax.annotation.Nullable;

/**
 * Rxify of {@link com.querydsl.core.dml.StoreClause}
 */
public interface RxStoreClause<C extends RxStoreClause<C>> extends RxDMLClause<C> {

    <T> C set(Path<T> path, @Nullable T value);

    <T> C set(Path<T> path, Expression<? extends T> expression);

    <T> C setNull(Path<T> path);

    boolean isEmpty();
}
