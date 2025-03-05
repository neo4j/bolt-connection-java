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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v57;

import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.v54.MessageWriterV54;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackInput;
import org.neo4j.driver.internal.bolt.basicimpl.impl.packstream.PackOutput;

public class MessageFormatV57 implements MessageFormat {
    @Override
    public MessageFormat.Writer newWriter(PackOutput output, ValueFactory valueFactory) {
        return new MessageWriterV54(output, valueFactory);
    }

    @Override
    public Reader newReader(PackInput input, ValueFactory valueFactory) {
        return new MessageReaderV57(input, valueFactory);
    }
}
