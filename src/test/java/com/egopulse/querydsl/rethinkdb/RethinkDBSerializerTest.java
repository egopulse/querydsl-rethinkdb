package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.net.Cursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.egopulse.querydsl.rethinkdb.Helper.reql;
import static com.egopulse.querydsl.rethinkdb.TestUtils.withConnection;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RethinkDBSerializerTest {

    private static RethinkDB r = RethinkDB.r;
    private static Table persons = r.table("persons");

    private static final QPerson qPerson = QPerson.person;

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
    public void verifyDatabaseStateAssumption() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetchFirst(persons
                            .filter(author -> author.getField("name").eq("HuyLe"))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testEqual() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe").with("isHandsome", true)).run(conn);
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.eq("HuyLe")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.isHandsome.isTrue()))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testStringEmpty() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("id", "a_veryyy_unique_key").with("name", "")).run(conn);

            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.name.isEmpty()))
                            .run(conn)),
                    is(not(empty())));
        });
    }

    @Test
    public void testAnd() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe").with("isHandsome", true)).run(conn);

            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.name.eq("HuyLe")
                                    .and(qPerson.isHandsome.isTrue())))
                            .run(conn)),
                    is(not(empty())));
        });
    }

    @Test
    public void testNot() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke").with("isHandsome", false)).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.isHandsome.isTrue().not()))
                            .run(conn)),
                    hasEntry("name", "Duke"));
        });
    }

    @Test
    public void testOr() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyNguyen").with("isHandsome", false)).run(conn);

            // Verify assumption
            assertEquals((long) persons.filter(row ->
                            row.g("name").eq("HuyNguyen")
                                    .and(row.g("isHandsome").eq(true))).count().run(conn),
                    0L);

            assertThat(
                    persons.filter(reql(qPerson.name.eq("HuyNguyen")))
                            .count()
                            .run(conn),
                    is(1L));

            assertThat(
                    persons.filter(reql(qPerson.isHandsome.isFalse()))
                            .count()
                            .run(conn),
                    is(1L));
        });
    }

    @Test
    public void testNe() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe").with("isHandsome", false)).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.ne("Duke")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    @Ignore
    public void testStartsWith() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.startsWith("Huy")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    @Ignore
    public void testStartsWithIc() throws Exception {
    }

    @Test
    @Ignore
    public void testEqIgnoreCase() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testStringContains() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testMatches() throws Exception {
        throw new UnsupportedOperationException("unimplemented");

    }

    @Test
    @Ignore
    public void testMatchesIc() throws Exception {
        throw new UnsupportedOperationException("unimplemented");

    }

    @Test
    @Ignore
    public void testLike() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testBetween() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void test_In() throws Exception {
        throw new UnsupportedOperationException("unimplemented");

    }

    @Test
    @Ignore
    public void testNotIn() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testColIsEmpty() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testName() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testLt() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testGt() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testLoe() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testGoe() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testIsNull() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testContainsKey() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testNear() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testNearSphere() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testElemMatch() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Test
    @Ignore
    public void testIn() throws Exception {
        throw new UnsupportedOperationException("unimplemented");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> fetchFirst(Cursor<Object> cursor) {
        if (!(cursor.hasNext())) {
            throw new AssertionError("Empty result set");
        }
        return (Map<String, ?>) cursor.next();
    }

    @SuppressWarnings("unchecked")
    private static Collection<Map<String, ?>> fetch(Cursor<Object> cursor) {
        List<Map<String, ?>> ret = new ArrayList<>();
        for (Object o : cursor) {
            ret.add((Map<String, ?>) o);
        }
        return ret;
    }

}
