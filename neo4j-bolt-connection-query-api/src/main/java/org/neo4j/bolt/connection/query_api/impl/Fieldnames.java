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
package org.neo4j.bolt.connection.query_api.impl;

public final class Fieldnames {

    public static final String CYPHER_TYPE = "$type";
    public static final String CYPHER_VALUE = "_value";
    public static final String _LABELS = "_labels";
    public static final String LABELS = "labels";
    public static final String _ELEMENT_ID = "_element_id";
    public static final String ELEMENT_ID = "elementId";
    public static final String _START_NODE_ELEMENT_ID = "_start_node_element_id";
    public static final String START_NODE_ELEMENT_ID = "startNodeElementId";
    public static final String _END_NODE_ELEMENT_ID = "_end_node_element_id";
    public static final String END_NODE_ELEMENT_ID = "endNodeElementId";
    public static final String _RELATIONSHIP_TYPE = "_type";
    public static final String RELATIONSHIP_TYPE = "type";
    public static final String _PROPERTIES = "_properties";
    public static final String PROPERTIES = "properties";
    public static final String FIELDS_KEY = "fields";
    public static final String VALUES_KEY = "values";
    public static final String DATA_KEY = "data";
    public static final String BOOKMARKS_KEY = "bookmarks";

    public static final String NOTIFICATIONS_KEY = "notifications";
    public static final String QUERY_PLAN_KEY = "queryPlan";
    public static final String COUNTERS_KEY = "counters";

    public static final String PROFILE_KEY = "profiledQueryPlan";

    public static final String ERRORS_KEY = "errors";

    public static final String ERROR_KEY = "error";

    private Fieldnames() {}
}
