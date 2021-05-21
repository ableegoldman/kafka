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
package org.apache.kafka.streams.integration;

//TODO KAFKA-12648
public class NamedTopologyIntegrationTest {
    /**
     * Things to test:
     * 1. Verify changelog & repartition topics decorated with named topology
     * 2. App runs as expected with
     *         -multiple subtopologies
     *         -persistent state
     *         -multi-partition input & output topics
     *         -test input and verify output records
     *         -standbys
     * 3. Is the task assignment balanced? Does KIP-441/warmup replica placement work as intended?
     * 4. maybe unit or integration test to verify subscription/assignment info sent as expected?
     */
}
