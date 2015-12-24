package com.egopulse.querydsl.rethinkdb.domain;

import com.querydsl.core.annotations.QueryEntity;

import java.util.Date;

@QueryEntity
public class Dates extends AbstractEntity {

    private Date date;

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

}
