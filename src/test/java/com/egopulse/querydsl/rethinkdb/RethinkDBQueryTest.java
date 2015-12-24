package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.QueryModifiers;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.egopulse.querydsl.rethinkdb.TestUtils.withConnection;
import static com.egopulse.querydsl.rethinkdb.TestUtils.withReturnableConnection;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RethinkDBQueryTest {

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
            RethinkDBQuery<Map<String, Object>> query =
                    new RethinkDBQuery<>("persons", new ReturnableConnection(conn));
            List<Map<String, Object>> resultPersons = query.where(qPerson.name.isNotEmpty()).fetch();
            assertEquals(2, resultPersons.size());
        });
    }

    @Test
    public void testFetchFirst() throws Exception {
        withConnection(conn -> {
            RethinkDBQuery<Map<String, Object>> query =
                    new RethinkDBQuery<>("persons", new ReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchFirst();
            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testFetchOne() throws Exception {
        withConnection(conn -> {
            RethinkDBQuery<Map<String, Object>> query =
                    new RethinkDBQuery<>("persons", new ReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchOne();
            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test(expected = NonUniqueResultException.class)
    public void testFetchOne_should_throw() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke")).run(conn);
            RethinkDBQuery<Map<String, Object>> query =
                    new RethinkDBQuery<>("persons", new ReturnableConnection(conn));
            query.where(qPerson.name.isNotEmpty()).fetchOne();
        });
    }

    @Test
    public void testCount() throws Exception {
        withReturnableConnection(conn -> {
            assertThat(
                    new RethinkDBQuery<>("persons", conn)
                            .where(qPerson.name.eq("HuyLe"))
                            .fetchCount(),
                    is(1L));
        });
    }

    @Test
    public void testRestrict() throws Exception {
        withReturnableConnection(conn -> {
            assertThat(
                    new RethinkDBQuery<>("persons", conn)
                            .where(qPerson.name.eq("HuyLe"))
                            .fetchCount(),
                    is(1L));
        });
    }


}