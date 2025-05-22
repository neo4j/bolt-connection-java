/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The Neo4j Bolt Connection Query API implementation module.
 */
module org.neo4j.bolt.connection.query.api {
    exports org.neo4j.bolt.connection.query.api;
    exports org.neo4j.bolt.connection.query.api.impl;

    opens org.neo4j.bolt.connection.query.api;
    opens org.neo4j.bolt.connection.query.api.impl;

    requires transitive org.neo4j.bolt.connection;
    requires java.net.http;
    requires java.naming;
    requires com.fasterxml.jackson.jr.ob;
    requires java.sql;
    requires java.desktop;
}
