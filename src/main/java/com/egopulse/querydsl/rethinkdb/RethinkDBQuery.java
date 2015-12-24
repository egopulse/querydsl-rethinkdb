package com.egopulse.querydsl.rethinkdb;

import com.mysema.commons.lang.CloseableIterator;
import com.querydsl.core.*;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import javax.annotation.Nonnegative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.egopulse.querydsl.rethinkdb.QueryDSL2RethinkDBConverter.reql;

/**
 * TODO:
 * - Add non-blocking query fashion right into here
 * - DRY: extracts some repeated code to common base methods
 * - Use serializer directly instead of the static converter
 */
public class RethinkDBQuery<T> implements SimpleQuery<RethinkDBQuery<T>>, Fetchable<T> {

    private static final RethinkDB r = RethinkDB.r;
    private ReturnableConnection borrowedConnection;
    private RethinkDBSerializer serializer;
    private Table table;

    private QueryMixin<RethinkDBQuery<T>> queryMixin;

    public RethinkDBQuery(String table, ReturnableConnection borrowedConnection) {
        this.queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata(), false);
        this.borrowedConnection = borrowedConnection;
        this.table = RethinkDB.r.table(table);
        this.serializer = new RethinkDBSerializer();
    }

    public static <T> RethinkDBQuery<T> query(String table, ReturnableConnection borrowedConnection) {
        return new RethinkDBQuery<>(table, borrowedConnection);
    }

    public List<T> fetch() {
        List<T> ret = (List<T>) realize(run(borrowedConnection.getConnection()));
        borrowedConnection.returnToPool();
        return ret;
    }

    public T fetchFirst() {
        T ret = (T) realizeFirst(run(borrowedConnection.getConnection()));
        borrowedConnection.returnToPool();
        return ret;
    }

    @Override
    public T fetchOne() {
        Cursor cursor = run(borrowedConnection.getConnection());
        T result = (T) cursor.next();
        if (cursor.hasNext()) {
            throw new NonUniqueResultException();
        }
        borrowedConnection.returnToPool();
        return result;
    }

    @Override
    public CloseableIterator<T> iterate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResults<T> fetchResults() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fetchCount() {
        long ret = generateReql().count().run(borrowedConnection.getConnection());
        borrowedConnection.returnToPool();
        return ret;
    }

    @Override
    public RethinkDBQuery<T> limit(@Nonnegative long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public RethinkDBQuery<T> offset(@Nonnegative long offset) {
        // !!! Unimplemented
        return queryMixin.offset(offset);
    }

    @Override
    public RethinkDBQuery<T> restrict(QueryModifiers modifiers) {
        // !!! Offset is unimplemented
        return queryMixin.restrict(modifiers);
    }

    @Override
    public RethinkDBQuery<T> orderBy(OrderSpecifier<?>... o) {
        return queryMixin.orderBy(o);
    }

    @Override
    public <Q> RethinkDBQuery<T> set(ParamExpression<Q> param, Q value) {
        return queryMixin.set(param, value);
    }

    @Override
    public RethinkDBQuery<T> distinct() {
        return null;
    }

    @Override
    public RethinkDBQuery<T> where(Predicate... predicates) {
        return queryMixin.where(predicates);
    }

    private ReqlExpr generateReql() {
        ReqlExpr query = table;

        query = query.filter(reql(queryMixin.getMetadata().getWhere()));

        Long limit = queryMixin.getMetadata().getModifiers().getLimit();
        if (limit != null) {
            query = query.limit(limit);
        }

        Long offset = queryMixin.getMetadata().getModifiers().getOffset();
        if (offset != null) {
            // TODO:
            throw new UnsupportedOperationException("Offset is unimplemented");
        }

        List<OrderSpecifier<?>> orderBys = queryMixin.getMetadata().getOrderBy();
        if (orderBys != null) {
            throw new UnsupportedOperationException("orderBy is unimplemented");
        }

        return query;
    }

    private Cursor run(Connection connection) {
        return generateReql().run(connection);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> realizeFirst(Cursor<Object> cursor) {
        if (!(cursor.hasNext())) {
            throw new AssertionError("Empty result set");
        }
        return (Map<String, ?>) cursor.next();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, ?>> realize(Cursor<Object> cursor) {
        List<Map<String, ?>> ret = new ArrayList<>();
        for (Object o : cursor) {
            ret.add((Map<String, ?>) o);
        }
        return ret;
    }

}
