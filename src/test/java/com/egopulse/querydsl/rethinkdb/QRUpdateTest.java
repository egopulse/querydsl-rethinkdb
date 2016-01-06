package com.egopulse.querydsl.rethinkdb;

import com.egopulse.querydsl.rethinkdb.domain.QPerson;
import com.egopulse.querydsl.rethinkdb.helper.DummyReturnableConnection;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static com.egopulse.querydsl.rethinkdb.TestUtils.fetch;
import static com.egopulse.querydsl.rethinkdb.TestUtils.withConnection;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class QRUpdateTest {

    private static final RethinkDB r = RethinkDB.r;
    private static final QPerson qPerson = QPerson.person;
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
    public void testExecute() throws Exception {
        withConnection(conn -> {
            new QRUpdate(persons, new DummyReturnableConnection(conn))
                    .set(qPerson.name, "HuyDepTrai")
                    .execute()
                    .toBlocking()
                    // TODO: is it necessary to call value()?
                    .value();
            Object result = fetch(persons.filter(row -> row.g("name").eq("HuyDepTrai")).run(conn));
            System.out.println("Result: " + result);
            assertThat(
                    fetch(persons.filter(row -> row.g("name").eq("HuyDepTrai")).run(conn)),
                    hasSize(1));
        });
    }
}