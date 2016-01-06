package com.egopulse.querydsl.rethinkdb.core;

import com.querydsl.core.FilteredClause;
import com.querydsl.core.types.Path;

import java.util.List;

/**
 * Rxify of {@link com.querydsl.core.dml.UpdateClause}
 */
public interface RxUpdateClause<C extends RxUpdateClause<C>> extends RxStoreClause<C>, FilteredClause<C> {

    C set(List<? extends Path<?>> paths, List<?> values);

}
