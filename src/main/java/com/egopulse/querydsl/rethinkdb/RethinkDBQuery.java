package com.egopulse.querydsl.rethinkdb;

import com.mysema.commons.lang.CloseableIterator;
import com.querydsl.core.*;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Cursor;

import javax.annotation.Nonnegative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.egopulse.querydsl.rethinkdb.QueryDSL2RethinkDBConverter.reql;

/**
 * TODO:
 *   - Add non-blocking query fashion right into here
 *   - DRY: extracts some repeated code to common base methods
 */
public class RethinkDBQuery<T> implements SimpleQuery<RethinkDBQuery<T>>, Fetchable<T> {

    private static final RethinkDB r = RethinkDB.r;
    private ReturnableConnection borrowedConnection;
    private Table table;

    private QueryMixin<RethinkDBQuery<T>> queryMixin;

    public RethinkDBQuery(String table, ReturnableConnection borrowedConnection) {
        this.queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata(), false);
        this.borrowedConnection = borrowedConnection;
        this.table = RethinkDB.r.table(table);
    }

    public static <T> RethinkDBQuery<T> query(String table, ReturnableConnection borrowedConnection) {
        return new RethinkDBQuery<>(table, borrowedConnection);
    }

    @Override
    public List<T> fetch() {
        Cursor result = table
                .filter(reql(queryMixin.getMetadata().getWhere()))
                .run(borrowedConnection.getConnection());
        borrowedConnection.returnToPool();
        return (List<T>) iterate(result);
    }

    @Override
    public T fetchFirst() {
        Cursor result = table
                .filter(reql(queryMixin.getMetadata().getWhere()))
                .limit(1)
                .run(borrowedConnection.getConnection());
        borrowedConnection.returnToPool();
        return (T) iterateFirst(result);
    }

    @Override
    public T fetchOne() {
        return null;
    }

    @Override
    public CloseableIterator<T> iterate() {
        return null;
    }

    @Override
    public QueryResults<T> fetchResults() {
        return null;
    }

    @Override
    public long fetchCount() {
        return 0;
    }

    @Override
    public RethinkDBQuery<T> limit(@Nonnegative long limit) {
        return null;
    }

    @Override
    public RethinkDBQuery<T> offset(@Nonnegative long offset) {
        return null;
    }

    @Override
    public RethinkDBQuery<T> restrict(QueryModifiers modifiers) {
        return null;
    }

    @Override
    public RethinkDBQuery<T> orderBy(OrderSpecifier<?>... o) {
        return null;
    }

    @Override
    public <Q> RethinkDBQuery<T> set(ParamExpression<Q> param, Q value) {
        return null;
    }

    @Override
    public RethinkDBQuery<T> distinct() {
        return null;
    }

    @Override
    public RethinkDBQuery<T> where(Predicate... predicates) {
        return queryMixin.where(predicates);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> iterateFirst(Cursor<Object> cursor) {
        if (!(cursor.hasNext())) {
            throw new AssertionError("Empty result set");
        }
        return (Map<String, ?>) cursor.next();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, ?>> iterate(Cursor<Object> cursor) {
        List<Map<String, ?>> ret = new ArrayList<>();
        for (Object o : cursor) {
            ret.add((Map<String, ?>) o);
        }
        return ret;
    }

}
