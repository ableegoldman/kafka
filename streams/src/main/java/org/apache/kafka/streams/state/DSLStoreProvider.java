package org.apache.kafka.streams.state;

import java.time.Duration;

public interface DSLStoreProvider {

    KeyValueBytesStoreSupplier keyValueStore(final String name);

    WindowBytesStoreSupplier windowStore(final String name, final Duration retentionPeriod, final Duration windowSize);

    SessionBytesStoreSupplier sessionStore(final String name, final Duration retentionPeriod);


}
