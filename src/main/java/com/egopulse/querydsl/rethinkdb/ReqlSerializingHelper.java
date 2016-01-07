package com.egopulse.querydsl.rethinkdb;

import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Predicate;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import com.rethinkdb.gen.ast.Table;

import java.util.List;

public class ReqlSerializingHelper {

    private static RethinkDBSerializer serializer = new RethinkDBSerializer();

    public static ReqlFunction1 reql(Expression<?> expr) {
        return (ReqlExpr row) -> serializer.handle(expr, row);
    }

    public static ReqlExpr reql(Table table, QueryMixin<?> queryMixin) {
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

}
