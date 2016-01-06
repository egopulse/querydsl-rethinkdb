package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.core.RxUpdateClause;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import rx.Single;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.egopulse.querydsl.rethinkdb.ReqlSerializingHelper.reql;

public class QRUpdate extends QROperation implements RxUpdateClause<QRUpdate> {

    private static final RethinkDB r = RethinkDB.r;

    private Map<Path, Object> updateValues = new HashMap<>();
    private List<Predicate> predicates;

    public QRUpdate(Table table, ReturnableConnection borrowedConnection) {
        super(table, borrowedConnection);
    }

    @Override
    public QRUpdate set(List<? extends Path<?>> paths, List<?> values) {
        if (paths.size() != values.size()) {
            throw new AssertionError("Mistmaches fields and values' size");
        }
        for (int i = 0; i < paths.size(); i++) {
            updateValues.put(paths.get(i), values.get(i));
        }
        return this;
    }

    @Override
    public QRUpdate where(Predicate... o) {
        predicates = Arrays.asList(o);
        return this;
    }

    @Override
    public <T> QRUpdate set(Path<T> path, @Nullable T value) {
        updateValues.put(path, value);
        return this;
    }

    @Override
    public <T> QRUpdate set(Path<T> path, Expression<? extends T> expression) {
        updateValues.put(path, reql(expression));
        return this;
    }

    @Override
    public <T> QRUpdate setNull(Path<T> path) {
        // TODO:
        throw new UnsupportedOperationException("To be implemented");
    }

    @Override
    public boolean isEmpty() {
        // TODO: WTH is this supposed todo
        throw new UnsupportedOperationException("To be implemented");
    }

    @Override
    public Single<Map<String, Long>> execute() {
        ReqlExpr query = getTable();

        if (predicates != null) {
            query = query.filter(reql(and(predicates)));
        }

        if (!(updateValues.isEmpty())) {
            for (Map.Entry<Path, Object> entry : updateValues.entrySet()) {
                query = query.update(row ->
                        r.hashMap(entry.getKey().getMetadata().getName(), entry.getValue()));
            }
        }

        final ReqlExpr finalQuery = query;

        return getBorrowedConnection()
                .getConnection()
                .compose(defaultTransformer())
                .map(conn -> finalQuery.run(conn));
    }

    private static Predicate and(List<Predicate> predicates) {
        Predicate merged = predicates.get(0);
        for (int i = 0; i < predicates.size(); i++) {
            merged = ExpressionUtils.and(merged, predicates.get(i));
        }
        return merged;
    }

}
