package com.egopulse.querydsl.rethinkdb.domain;

import com.egopulse.querydsl.rethinkdb.annotation.Embedded;
import com.querydsl.core.annotations.QueryEntity;

import java.util.HashMap;
import java.util.Map;

@QueryEntity
public class MapEntity extends AbstractEntity {

    @Embedded
    private Map<String, String> properties = new HashMap<String, String>();

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

}
