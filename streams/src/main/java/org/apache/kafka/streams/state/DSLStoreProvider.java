package org.apache.kafka.streams.state;

import java.time.Duration;

public interface DSLStoreProvider {

    KeyValueBytesStoreSupplier keyValueStore(final String name);

    default KeyValueBytesStoreSupplier timestampedKeyValueStore(final String name) {
        return keyValueStore(name);
    }

    WindowBytesStoreSupplier windowStore(final String name, final Duration retentionPeriod, final Duration windowSize);

    default WindowBytesStoreSupplier timestampedWindowStore(final String name, final Duration retentionPeriod, final Duration windowSize, final boolean retainDuplicates) {
        return windowStore(name, retentionPeriod, windowSize);
    }

    SessionBytesStoreSupplier sessionStore(final String name, final Duration retentionPeriod);

    default SessionBytesStoreSupplier timestampedSessionStore(final String name, final Duration retentionPeriod) {
        return sessionStore(name, retentionPeriod);
    }

}
