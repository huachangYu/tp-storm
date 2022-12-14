/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.trident.windowing.strategy;

import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;

/**
 * StrategyType for windowing which will have respective trigger and eviction policies.
 */
public interface WindowStrategy<T> {

    /**
     * Returns a {@code TriggerPolicy}  by creating with {@code triggerHandler} and {@code evictionPolicy} with the given configuration.
     */
    TriggerPolicy<T, ?> getTriggerPolicy(TriggerHandler triggerHandler, EvictionPolicy<T, ?> evictionPolicy);

    /**
     * Returns an {@code EvictionPolicy} instance for this strategy with the given configuration.
     */
    EvictionPolicy<T, ?> getEvictionPolicy();
}
