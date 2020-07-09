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

import java.util.LinkedList;
import java.util.function.BiFunction;

public final class ExceptionUtils {
    private ExceptionUtils() {}

    @FunctionalInterface
    public interface ExceptionalRunnable {
        void run() throws Exception;
    }

    public static LinkedList<Exception> executeAll(final ExceptionalRunnable... actions) {
        final LinkedList<Exception> suppressed = new LinkedList<>();
        for (final ExceptionalRunnable action : actions) {
            try {
                action.run();
            } catch (final Exception exception) {
                suppressed.add(exception);
            }
        }
        return suppressed;
    }

    public static void throwSuppressed(final String message, final LinkedList<Exception> suppressed) {
        throwSuppressed(RuntimeException::new, message, suppressed);
    }

    public static <E extends RuntimeException> void throwSuppressed(final BiFunction<String, Exception, E> exceptionConstructor,
                                       final String message,
                                       final LinkedList<Exception> suppressed) {
        if (!suppressed.isEmpty()) {
            final E toThrow = getException(exceptionConstructor, message, suppressed);
            throw toThrow;
        }
    }

    public static <E extends RuntimeException> E getException(final BiFunction<String, Exception, E> exceptionConstructor, final String message, final LinkedList<Exception> suppressed) {
        final Exception firstCause = suppressed.pollFirst();
        final E toThrow = exceptionConstructor.apply(message, firstCause);
        for (final Exception e : suppressed) {
            toThrow.addSuppressed(e);
        }
        return toThrow;
    }
}
