package com.egopulse.querydsl.rethinkdb.annotation;

public @interface Reference {

    String value() default "noname";

}
