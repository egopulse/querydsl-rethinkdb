/*
 * Copyright 2015, The Querydsl Team (http://www.querydsl.com/team)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.egopulse.querydsl.rethinkdb.domain2;

import com.egopulse.querydsl.rethinkdb.annotation.Embedded;
import com.egopulse.querydsl.rethinkdb.annotation.Entity;
import com.querydsl.core.annotations.QueryEntity;

import java.util.Map;

// TODO: verify if custom collection name is necessary
//@Entity(value = "user")
@QueryEntity
public class User {

    @Embedded
    Map<String, UserAttribute> properties;

}