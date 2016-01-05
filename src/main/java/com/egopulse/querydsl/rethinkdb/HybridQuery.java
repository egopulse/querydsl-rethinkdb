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
import rx.schedulers.Schedulers;

import javax.annotation.Nonnegative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.egopulse.querydsl.rethinkdb.Helper.reql;

/**
 * TODO:
 * - Add non-blocking query fashion right into here
 * - DRY: extracts some repeated code to common base methods
 * - Use serializer directly instead of the static converter
 */
public class HybridQuery<T> implements SimpleQuery<HybridQuery<T>>, RxFetchable<T> {

    private ReturnableConnection borrowedConnection;
    private RethinkDBSerializer serializer;
    private Table table;
    private QueryMixin<HybridQuery<T>> queryMixin;

    // Query is executed in parallel on shards,
    // hence the returned after `run()` is fully realized data instead of a cursor
    private boolean isParallelized = true;

    public HybridQuery(EntityPath<?> table, ReturnableConnection borrowedConnection) {
        this(table.getMetadata().getName() + "s", borrowedConnection);
    }

    public HybridQuery(String table, ReturnableConnection borrowedConnection) {
        this.queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata(), false);
        this.borrowedConnection = borrowedConnection;
        this.table = RethinkDB.r.table(table);
        this.serializer = new RethinkDBSerializer();
    }

    private <Q> Single.Transformer<Q, Q> defaultTransformer() {
        return single -> single
                .toObservable()
                .subscribeOn(Schedulers.io())
                .finallyDo(() -> borrowedConnection.handBack())
                .toSingle();
    }

    @Override
    public Single<List<T>> fetch() {
        return borrowedConnection
                .getConnection()
                .map(conn -> (List<T>) realize(run(conn)))
                .compose(defaultTransformer());
    }

    @Override
    public Single<T> fetchFirst() {
        return borrowedConnection
                .getConnection()
                .map(conn -> (T) realizeAtom(run(conn), false))
                .compose(defaultTransformer());
    }

    @Override
    public Single<T> fetchOne() {
        return borrowedConnection
                .getConnection()
                .map(conn -> (T) realizeAtom(run(conn), true))
                .compose(defaultTransformer());
    }

    @Override
    public Single<Long> fetchCount() {
        return borrowedConnection
                .getConnection()
                .map(conn -> {
                    long ret = generateReql().count().run(conn);
                    borrowedConnection.handBack();
                    return ret;
                })
                .compose(defaultTransformer());
    }

    @Override
    public HybridQuery<T> limit(@Nonnegative long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public HybridQuery<T> offset(@Nonnegative long offset) {
        return queryMixin.offset(offset);
    }

    @Override
    public HybridQuery<T> restrict(QueryModifiers modifiers) {
        // !!! restrict include offset which is unimplemented
        return queryMixin.restrict(modifiers);
    }

    @Override
    public HybridQuery<T> orderBy(OrderSpecifier<?>... o) {
        isParallelized = false;
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

        Predicate whereClause = queryMixin.getMetadata().getWhere();
        if (whereClause != null) {
            query = query.filter(reql(queryMixin.getMetadata().getWhere()));
        }

        Long limit = queryMixin.getMetadata().getModifiers().getLimit();
        if (limit != null) {
            query = query.limit(limit);
        }

        Long offset = queryMixin.getMetadata().getModifiers().getOffset();
        if (offset != null) {
            query = query.skip(offset);
        }

        List<OrderSpecifier<?>> orderBys = queryMixin.getMetadata().getOrderBy();
        if (!orderBys.isEmpty()) {
            query = serializer.toSort(orderBys, query);
        }

        return query;
    }

    private Object run(Connection connection) {
        return generateReql().run(connection);
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> realizeAtom(Object queryResult, boolean ensureAtomValue) {
        if (isParallelized) {
            Cursor cursor = (Cursor) queryResult;
            if (!(cursor.hasNext())) {
                throw new AssertionError("Empty result set");
            }
            Map<String, ?> ret = (Map<String, ?>) cursor.next();
            if (ensureAtomValue && cursor.hasNext()) {
                throw new NonUniqueResultException();
            }
            return ret;
        } else {
            return ((List<Map<String, ?>>) queryResult).get(0);
        }

    }

    @SuppressWarnings("unchecked")
    private List<Map<String, ?>> realize(Object queryResult) {
        if (isParallelized) {
            Cursor cursor = (Cursor) queryResult;
            List<Map<String, ?>> ret = new ArrayList<>();
            for (Object o : cursor) {
                ret.add((Map<String, ?>) o);
            }
            return ret;
        } else {
            return (List<Map<String, ?>>) queryResult;
        }
    }

}
