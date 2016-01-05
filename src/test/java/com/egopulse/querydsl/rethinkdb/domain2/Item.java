package com.egopulse.querydsl.rethinkdb.domain2;

import com.querydsl.core.annotations.QueryEntity;

@QueryEntity
public class Item {

    String name;
    int col1;
    int col2;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCol1() {
        return col1;
    }

    public void setCol1(int col1) {
        this.col1 = col1;
    }

    public int getCol2() {
        return col2;
    }

    public void setCol2(int col2) {
        this.col2 = col2;
    }
}
