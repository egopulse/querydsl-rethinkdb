package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.NonUniqueResultException;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.net.Cursor;
import rx.Single;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QROperation {

    private ReturnableConnection borrowedConnection;
    private Table table;
    protected boolean isParallelized = true;

    public QROperation(Table table, ReturnableConnection borrowedConnection) {
        this.borrowedConnection = borrowedConnection;
        this.table = table;
    }

    public ReturnableConnection getBorrowedConnection() {
        return borrowedConnection;
    }

    public Table getTable() {
        return table;
    }

    protected <Q> Single.Transformer<Q, Q> defaultTransformer() {
        return single -> single
                .toObservable()
                .subscribeOn(Schedulers.io())
                .finallyDo(() -> getBorrowedConnection().handBack())
                .toSingle();
    }

    @SuppressWarnings("unchecked")
    protected Map<String, ?> realizeAtom(Object queryResult, boolean ensureAtomValue) {
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
    protected List<Map<String, ?>> realize(Object queryResult) {
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
