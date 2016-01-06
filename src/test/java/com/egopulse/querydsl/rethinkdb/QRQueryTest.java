package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.egopulse.querydsl.rethinkdb.domain2.QItem;
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
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class QRQueryTest {

    private static final QPerson qPerson = QPerson.person;
    private static final QItem qItem = QItem.item;

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
            persons.delete().run(conn);
        });
    }

    @Test
    public void testFetch() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke")).run(conn);

            QRQuery<Map<String, Object>> query =
                    new QRQuery<>("persons", new DummyReturnableConnection(conn));
            Single<List<Map<String, Object>>> resultPersons = query.where(qPerson.name.isNotEmpty()).fetch();

            assertThat(
                    resultPersons.toBlocking().value().size(),
                    equalTo(2));
        });
    }

    @Test
    public void testFetchFirst() throws Exception {
        withConnection(conn -> {
            QRQuery<Map<String, Object>> query =
                    new QRQuery<>("persons", new DummyReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchFirst().toBlocking().value();

            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test
    public void testFetchOne() throws Exception {
        withConnection(conn -> {
            QRQuery<Map<String, Object>> query =
                    new QRQuery<>("persons", new DummyReturnableConnection(conn));
            Map<String, Object> huy = query.where(qPerson.name.eq("HuyLe")).fetchOne().toBlocking().value();

            assertThat(huy, hasEntry("name", "HuyLe"));
        });
    }

    @Test(expected = NonUniqueResultException.class)
    public void testFetchOne_should_throw() throws Exception {
        withConnection(conn -> {
            persons.insert(r.hashMap("name", "Duke")).run(conn);
            QRQuery<Map<String, Object>> query =
                    new QRQuery<>("persons", new DummyReturnableConnection(conn));
            query.where(qPerson.name.isNotEmpty()).fetchOne().toBlocking().value();
        });
    }

    @Test
    public void testCount() throws Exception {
        withReturnableConnection(conn -> {
            assertThat(
                    new QRQuery<>("persons", conn)
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
                    new QRQuery<>("persons", conn)
                            .where(qPerson.name.eq("HuyLe"))
                            .fetchCount()
                            .toBlocking()
                            .value(),
                    is(1L));
        });
    }

    @Test
    public void testOrderBy() throws Exception {
        withConnection(conn ->
                persons.insert(r.hashMap("name", "HuyNguyen")).run(conn));

        withReturnableConnection(conn ->
                assertThat(
                        new QRQuery<Map<String, ?>>("persons", conn)
                                .orderBy(qPerson.name.asc())
                                .fetchFirst()
                                .toBlocking()
                                .value(),
                        hasEntry("name", "HuyLe")));

        withReturnableConnection(conn ->
                assertThat(
                        new QRQuery<Map<String, ?>>("persons", conn)
                                .orderBy(qPerson.name.desc())
                                .fetchFirst()
                                .toBlocking()
                                .value(),
                        hasEntry("name", "HuyNguyen")));
    }

    @Test
    public void testOrderBy_nested() throws Exception {
        withConnection(conn -> {
            try {
                r.tableDrop("items").run(conn);
            } catch (ReqlOpFailedError e) { } // Assume this exception caused by the table `items` already exists

            r.tableCreate("items").run(conn);
            r.table("items").insert(r.hashMap("name", "a").with("col1", 1).with("col2", 3)).run(conn);
            r.table("items").insert(r.hashMap("name", "b").with("col1", 2).with("col2", 2)).run(conn);
            r.table("items").insert(r.hashMap("name", "c").with("col1", 2).with("col2", 1)).run(conn);
        });

        withReturnableConnection(conn -> {
            assertThat(
                    new QRQuery<Map<String, ?>>("items", conn)
                            .orderBy(qItem.col1.asc(), qItem.col2.asc())
                            .fetch()
                            .toBlocking()
                            .value(),
                    contains(hasEntry("name", "a"), hasEntry("name", "c"), hasEntry("name", "b")));
        });
    }

//    @Test
//    public void testSet() throws Exception {
//        withReturnableConnection(conn ->
//                new QRQuery<Map<String, ?>>("persons", conn)
//                        .where(qPerson.name.eq("HuyLe"))
//                        .set(qPerson.name, "HuyDepTraiHaoHoaPhongNha")
//                        .fet
//        );
//    }

}