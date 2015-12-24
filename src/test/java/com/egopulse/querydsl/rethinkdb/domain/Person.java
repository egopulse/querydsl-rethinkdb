package com.egopulse.querydsl.rethinkdb.domain;

import com.egopulse.querydsl.rethinkdb.annotation.Id;
import com.egopulse.querydsl.rethinkdb.type.ObjectId;
import com.querydsl.core.annotations.QueryEntity;

@QueryEntity
class Person {
   @Id
   public ObjectId id;

   public String name;

   public boolean isHandsome;

   // manual reference to an address
   public ObjectId addressId;
}