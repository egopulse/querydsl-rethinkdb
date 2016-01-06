package com.egopulse.querydsl.rethinkdb.domain;

import com.egopulse.querydsl.rethinkdb.annotation.Id;
import com.egopulse.querydsl.rethinkdb.type.ObjectId;
import com.querydsl.core.annotations.QueryEntity;

import java.util.List;

@QueryEntity
class Person {
   @Id
   public ObjectId id;

   public String name;

   public boolean isHandsome;

   public List<String> nicknames;

   public Integer age;

   // manual reference to an address
   public ObjectId addressId;
}