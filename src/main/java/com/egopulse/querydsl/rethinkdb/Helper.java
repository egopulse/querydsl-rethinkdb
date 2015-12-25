package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.types.Expression;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;

class Helper {

    private static RethinkDBSerializer serializer = new RethinkDBSerializer();

    public static ReqlFunction1 reql(Expression<?> predicate) {
        return (ReqlExpr row) -> serializer.handle(predicate, row);
    }
}
