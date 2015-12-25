package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.egopulse.querydsl.rethinkdb.helper.DummyReturnableConnection;
import com.querydsl.core.NonUniqueResultException;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Single;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.egopulse.querydsl.rethinkdb.TestUtils.withConnection;
import static com.egopulse.querydsl.rethinkdb.TestUtils.withReturnableConnection;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;

public class HybridQueryTest {

    private static final QPerson qPerson = QPerson.person;

    private static final RethinkDB r = RethinkDB.r;
    private static final Table persons = r.table("persons");

    @Before
    public void setUp() throws TimeoutException {
        withConnection(conn -> {
            try {
                r.tableDrop("persons").run(conn);
            } catch (ReqlOpFailedError supposedTableExists) {
            }
            r.tableCreate("persons").run(conn);
            r.table("persons").insert(r.hashMap("name", "HuyLe")).run(conn);
        });
    }

    @After
    public void tearDown() throws TimeoutException {
        withConnection(conn -> {
            r.tableDrop("persons").run(conn);
        });
    }

    @Test
    public void testFetch() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke")).run(conn);

            HybridQuery<Map<String, Object>> query =
                    new HybridQuery<>("persons", new DummyReturnableConnection(conn));
            Single<List<Map<String, Object>>> resultPersons = query.where(qPerson.name.isNotEmpty()).fetch();

            assertThat(
                    resultPersons.toBlocking().value().size(),
                    equalTo(2));
        });
    }

    @Test
    public void testFetchFirst() throws Exception {
        withConnection(conn -> {
            HybridQuery<Map<String, Object>> query =
                    new HybridQuery<>("persons", new DummyReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchFirst().toBlocking().value();

            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testFetchOne() throws Exception {
        withConnection(conn -> {
            HybridQuery<Map<String, Object>> query =
                    new HybridQuery<>("persons", new DummyReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchOne().toBlocking().value();

            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test(expected = NonUniqueResultException.class)
    public void testFetchOne_should_throw() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke")).run(conn);
            HybridQuery<Map<String, Object>> query =
                    new HybridQuery<>("persons", new DummyReturnableConnection(conn));
            query.where(qPerson.name.isNotEmpty()).fetchOne().toBlocking().value();
        });
    }

    @Test
    public void testCount() throws Exception {
        withReturnableConnection(conn -> {
            assertThat(
                    new HybridQuery<>("persons", conn)
                            .where(qPerson.name.eq("HuyLe"))
                            .fetchCount()
                            .toBlocking()
                            .value(),
                    is(1L));
        });
    }

    @Test
    public void testRestrict() throws Exception {
        withReturnableConnection(conn -> {
            assertThat(
                    new HybridQuery<>("persons", conn)
                            .where(qPerson.name.eq("HuyLe"))
                            .fetchCount()
                            .toBlocking()
                            .value(),
                    is(1L));
        });
    }

}