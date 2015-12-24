package com.egopulse.querydsl.rethinkdb.annotation;

public @interface Entity {

    String value() default "noname";

}
