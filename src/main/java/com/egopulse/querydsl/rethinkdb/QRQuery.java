package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.core.RxFetchable;
import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.QueryModifiers;
import com.querydsl.core.SimpleQuery;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.net.Connection;
import rx.Single;

import javax.annotation.Nonnegative;
import java.util.List;

import static com.egopulse.querydsl.rethinkdb.ReqlSerializingHelper.reql;

/*
 * TODO: type-safety
 */
public class QRQuery<T> extends QROperation implements SimpleQuery<QRQuery<T>>, RxFetchable<T> {

    private static final RethinkDB r = RethinkDB.r;

    private QueryMixin<QRQuery<T>> queryMixin;

    // Query is executed in parallel on shards,
    // hence the returned after `run()` is fully realized data instead of a cursor

    public QRQuery(EntityPath<?> table, ReturnableConnection borrowedConnection) {
        // FIXME: add `s` for table is a grammatically error prone convention
        this(table.getMetadata().getName() + "s", borrowedConnection);
    }

    public QRQuery(String tableName, ReturnableConnection borrowedConnection) {
        super(r.table(tableName), borrowedConnection);
        this.queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata(), false);
    }


    @Override
    public Single<List<T>> fetch() {
        return getBorrowedConnection()
                .getConnection()
                .map(conn -> (List<T>) realize(run(conn)))
                .compose(defaultTransformer());
    }

    @Override
    public Single<T> fetchFirst() {
        return getBorrowedConnection()
                .getConnection()
                .map(conn -> (T) realizeAtom(run(conn), false))
                .compose(defaultTransformer());
    }

    @Override
    public Single<T> fetchOne() {
        return getBorrowedConnection()
                .getConnection()
                .map(conn -> (T) realizeAtom(run(conn), true))
                .compose(defaultTransformer());
    }

    @Override
    public Single<Long> fetchCount() {
        return getBorrowedConnection()
                .getConnection()
                .map(conn -> {
                    long ret = generateReql().count().run(conn);
                    getBorrowedConnection().handBack();
                    return ret;
                })
                .compose(defaultTransformer());
    }

    @Override
    public QRQuery<T> limit(@Nonnegative long limit) {
        return queryMixin.limit(limit);
    }

    @Override
    public QRQuery<T> offset(@Nonnegative long offset) {
        return queryMixin.offset(offset);
    }

    @Override
    public QRQuery<T> restrict(QueryModifiers modifiers) {
        // !!! restrict include offset which is unimplemented
        return queryMixin.restrict(modifiers);
    }

    @Override
    public QRQuery<T> orderBy(OrderSpecifier<?>... o) {
        isParallelized = false;
        return queryMixin.orderBy(o);
    }

    @Override
    public <Q> QRQuery<T> set(ParamExpression<Q> param, Q value) {
        throw new UnsupportedOperationException("Unimplemented");
//        return queryMixin.set(param, value);
    }

    @Override
    public QRQuery<T> distinct() {
        return null;
    }

    @Override
    public QRQuery<T> where(Predicate... predicates) {
        return queryMixin.where(predicates);
    }

    private ReqlExpr generateReql() {
        return reql(getTable(), queryMixin);
    }

    private Object run(Connection connection) {
        return generateReql().run(connection);
    }


}
