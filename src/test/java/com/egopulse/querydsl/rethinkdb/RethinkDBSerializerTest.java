package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Cursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.*;
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
            r.table("persons").insert(r
                    .hashMap("name", "HuyLe")
                    .with("age", 27)
                    .with("nicknames", Collections.emptyList())
            ).run(conn);
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
    public void testStartsWithIgnoreCase() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.startsWithIgnoreCase("huy")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testEndsWith() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.endsWith("Le")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testEndsWithIgnoreCase() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.endsWithIgnoreCase("le")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testEqIgnoreCase() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.equalsIgnoreCase("huyle")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testStringContains() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.containsIgnoreCase("yL")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testMatches() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.matches("^Huy")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    @Ignore
    public void testMatchesIc() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyLe")).run(conn);

            assertThat(
                    fetchFirst(persons
                            // FIXME: What is the corresonding method call of Ops.STRING_CONTAINS_IC
//                            .filter(reql(qPerson.name.matches("^huy")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testLike() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.name.like("%yL%")))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testBetween() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Anh Huy Gia").with("age", 90)).run(conn);

            Object result = fetch(persons
                    .filter(reql(qPerson.age.between(80, 100)))
                    .run(conn));
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.age.between(80, 100)))
                            .run(conn)),
                    hasEntry("name", "Anh Huy Gia"));
        });
    }

    @Test
    public void testIn() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyNguyen").with("age", 90L)).run(conn);

            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.name.in("HuyLe", "HuyNguyen")))
                            .run(conn)),
                    hasSize(2));
        });
    }

    @Test
    public void testNotIn() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "HuyNguyen").with("age", 90L)).run(conn);

            assertThat(
                    fetchFirst(persons
                            // TODO: notIn("HuyLe") will fleky
                            .filter(reql(qPerson.name.notIn("HuyLe", "Whoever")))
                            .run(conn)),
                    hasEntry("name", "HuyNguyen"));
        });
    }

    @Test
    public void testColIsEmpty() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.nicknames.isEmpty()))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }


    @Test
    public void testLt() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.age.lt(30)))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.age.lt(1)))
                            .run(conn)),
                    empty());
        });
    }

    @Test
    public void testGt() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.age.gt(30)))
                            .run(conn)),
                    empty());
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.age.gt(1)))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testLoe() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.age.loe(1)))
                            .run(conn)),
                    empty());
            assertThat(
                    fetchFirst(persons
                            .filter(reql(qPerson.age.loe(27)))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testGoe() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.age.goe(100)))
                            .run(conn)),
                    empty());
            assertThat(
//                    fetchFirst(persons.filter(row -> row.g("age").gt(27).or(row.g("age").eq(27))).run(conn)),
                    fetchFirst(persons
                            .filter(reql(qPerson.age.goe(27)))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testIsNull() throws Exception {
        withConnection(conn -> {
            assertThat(
//                    fetchFirst(persons.filter(row -> row.hasFields("addressId").not()).run(conn)),
                    fetchFirst(persons
                            .filter(reql(qPerson.addressId.isNull()))
                            .run(conn)),
                    hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testIsNotNull() throws Exception {
        withConnection(conn -> {
            assertThat(
                    fetch(persons
                            .filter(reql(qPerson.addressId.isNotNull()))
                            .run(conn)),
                    empty());
        });
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

    private String toJsonString(ReqlExpr expr) throws UnsupportedEncodingException {
        Query q = Query.start(0L, expr, new OptArgs());
        return new String(q.serialize().array(), "UTF-8");
    }

}
