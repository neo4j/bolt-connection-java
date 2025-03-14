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
package org.neo4j.bolt.connection.netty.impl.messaging.request;

import java.util.Map;
import org.neo4j.bolt.connection.NotificationClassification;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

abstract class MessageWithMetadata implements Message {
    static final String NOTIFICATIONS_MINIMUM_SEVERITY = "notifications_minimum_severity";
    static final String NOTIFICATIONS_DISABLED_CATEGORIES = "notifications_disabled_categories";
    static final String NOTIFICATIONS_DISABLED_CLASSIFICATIONS = "notifications_disabled_classifications";
    private final Map<String, Value> metadata;

    public MessageWithMetadata(Map<String, Value> metadata) {
        this.metadata = metadata;
    }

    public Map<String, Value> metadata() {
        return metadata;
    }

    static void appendNotificationConfig(
            Map<String, Value> result,
            NotificationConfig config,
            boolean legacyNotifications,
            ValueFactory valueFactory) {
        if (config != null) {
            var severity = config.minimumSeverity();
            if (severity != null) {
                result.put(
                        NOTIFICATIONS_MINIMUM_SEVERITY,
                        valueFactory.value(severity.type().toString()));
            }
            var disabledClassifications = config.disabledClassifications();
            if (disabledClassifications != null) {
                var list = disabledClassifications.stream()
                        .map(NotificationClassification::type)
                        .map(Enum::toString)
                        .toList();
                result.put(
                        legacyNotifications
                                ? NOTIFICATIONS_DISABLED_CATEGORIES
                                : NOTIFICATIONS_DISABLED_CLASSIFICATIONS,
                        valueFactory.value(list));
            }
        }
    }
}
