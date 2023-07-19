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
