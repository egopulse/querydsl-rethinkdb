package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import rx.Single;

import javax.annotation.Nonnegative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.egopulse.querydsl.rethinkdb.FiberExecutor.execute;
import static com.egopulse.querydsl.rethinkdb.Helper.reql;

/**
 * TODO:
 * - Add non-blocking query fashion right into here
 * - DRY: extracts some repeated code to common base methods
 * - Use serializer directly instead of the static converter
 */
public class HybridQuery<T> implements SimpleQuery<HybridQuery<T>>, AsyncFetchable<T> {

    private static final RethinkDB r = RethinkDB.r;
    private ReturnableConnection borrowedConnection;
    private RethinkDBSerializer serializer;
    private Table table;
    private QueryMixin<HybridQuery<T>> queryMixin;

    public HybridQuery(EntityPath<?> table, ReturnableConnection borrowedConnection) {
        this(table.getMetadata().getName() + "s", borrowedConnection);
    }

    public HybridQuery(String table, ReturnableConnection borrowedConnection) {
        this.queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata(), false);
        this.borrowedConnection = borrowedConnection;
        this.table = RethinkDB.r.table(table);
        this.serializer = new RethinkDBSerializer();
    }

    private <Q> Single.Transformer<Q, Q> postFetch() {
        return single -> single
                .doOnError(err -> borrowedConnection.handBack())
                .doOnSuccess(__ -> borrowedConnection.handBack());
    }

    @Override
    public Single<List<T>> fetch() {
        return borrowedConnection
                .getConnection()
                .flatMap(conn ->
                        execute(() -> (List<T>) realize(run(conn))))
                .compose(postFetch());
    }

    @Override
    public Single<T> fetchFirst() {
        return borrowedConnection
                .getConnection()
                .flatMap(conn ->
                        execute(() -> (T) realizeFirst(run(conn))))
                .compose(postFetch());
    }

    @Override
    public Single<T> fetchOne() {
        return borrowedConnection
                .getConnection()
                .flatMap(conn ->
                        execute(() -> {
                            Cursor cursor = run(conn);
                            T result = (T) cursor.next();
                            if (cursor.hasNext()) {
                                throw new NonUniqueResultException();
                            }
                            return result;
                        }))
                .compose(postFetch());
    }

    @Override
    public Single<Long> fetchCount() {
        return borrowedConnection
                .getConnection()
                .flatMap(conn ->
                        execute(() -> {
                            long ret = generateReql().count().run(conn);
                            borrowedConnection.handBack();
                            return ret;
                        }))
                .compose(postFetch());
    }

    @Override
    public HybridQuery<T> limit(@Nonnegative long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public HybridQuery<T> offset(@Nonnegative long offset) {
        // !!! Unimplemented
        return queryMixin.offset(offset);
    }

    @Override
    public HybridQuery<T> restrict(QueryModifiers modifiers) {
        // !!! Offset is unimplemented
        return queryMixin.restrict(modifiers);
    }

    @Override
    public HybridQuery<T> orderBy(OrderSpecifier<?>... o) {
        return queryMixin.orderBy(o);
    }

    @Override
    public <Q> HybridQuery<T> set(ParamExpression<Q> param, Q value) {
        return queryMixin.set(param, value);
    }

    @Override
    public HybridQuery<T> distinct() {
        return null;
    }

    @Override
    public HybridQuery<T> where(Predicate... predicates) {
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
        if (!orderBys.isEmpty()) {
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