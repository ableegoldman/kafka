/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import java.time.Duration;
import org.apache.kafka.streams.state.DSLStoreProvider;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

public class RocksDBStoreProvider implements DSLStoreProvider {

    @Override
    public KeyValueBytesStoreSupplier keyValueStore(final String name) {
        return Stores.persistentKeyValueStore(name);
    }

    @Override
    public WindowBytesStoreSupplier windowStore(final String name,
                                                final Duration retentionPeriod,
                                                final Duration windowSize,
                                                final boolean retainDuplicates) {
        return Stores.persistentTimestampedWindowStore(name, retentionPeriod, windowSize, retainDuplicates);
    }

    @Override
    public SessionBytesStoreSupplier sessionStore(final String name, final Duration retentionPeriod) {
        return Stores.persistentSessionStore(name, retentionPeriod);
    }
}
