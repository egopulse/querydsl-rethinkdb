package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
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
import static org.hamcrest.Matchers.hasEntry;
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
    public void testFetchFirst() throws Exception {
        withConnection(conn -> {
            RethinkDBQuery<Map<String, Object>> query =
                    new RethinkDBQuery<>("persons", new ReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchFirst();
            assertThat(huy, hasEntry("name", "HuyLe"));
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


}