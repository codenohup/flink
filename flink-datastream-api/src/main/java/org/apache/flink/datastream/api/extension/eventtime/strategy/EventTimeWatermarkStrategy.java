/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.api.extension.eventtime.strategy;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.time.Duration;

/** Component which encapsulates the logic of how and when to extract event time and watermarks. */
@Experimental
public class EventTimeWatermarkStrategy<T> implements Serializable {
    // how to extract event time from event
    private EventTimeExtractor<T> eventTimeExtractor;

    // what frequency to generate event time watermark
    private EventTimeWatermarkGenerateMode generateMode = EventTimeWatermarkGenerateMode.PERIODIC;

    // if not set, it will default to the value of the "pipeline.auto-watermark-interval"
    // configuration.
    private Duration periodicWatermarkInterval = Duration.ZERO;

    // if set to zero, it will not generate idle status watermark
    private Duration idleTimeout = Duration.ZERO;

    // max out-of-order time
    private Duration maxOutOfOrderTime = Duration.ZERO;

    public EventTimeWatermarkStrategy(EventTimeExtractor<T> eventTimeExtractor) {
        this(
                eventTimeExtractor,
                EventTimeWatermarkGenerateMode.PERIODIC,
                Duration.ZERO,
                Duration.ZERO,
                Duration.ZERO);
    }

    public EventTimeWatermarkStrategy(
            EventTimeExtractor<T> eventTimeExtractor,
            EventTimeWatermarkGenerateMode generateMode,
            Duration periodicWatermarkInterval,
            Duration idleTimeout,
            Duration maxOutOfOrderTime) {
        this.eventTimeExtractor = eventTimeExtractor;
        this.generateMode = generateMode;
        this.periodicWatermarkInterval = periodicWatermarkInterval;
        this.idleTimeout = idleTimeout;
        this.maxOutOfOrderTime = maxOutOfOrderTime;
    }

    public EventTimeExtractor<T> getEventTimeExtractor() {
        return eventTimeExtractor;
    }

    public void setEventTimeExtractor(EventTimeExtractor<T> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
    }

    public EventTimeWatermarkGenerateMode getGenerateMode() {
        return generateMode;
    }

    public void setGenerateMode(EventTimeWatermarkGenerateMode generateMode) {
        this.generateMode = generateMode;
    }

    public Duration getPeriodicWatermarkInterval() {
        return periodicWatermarkInterval;
    }

    public void setPeriodicWatermarkInterval(Duration periodicWatermarkInterval) {
        this.periodicWatermarkInterval = periodicWatermarkInterval;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Duration getMaxOutOfOrderTime() {
        return maxOutOfOrderTime;
    }

    public void setMaxOutOfOrderTime(Duration maxOutOfOrderTime) {
        this.maxOutOfOrderTime = maxOutOfOrderTime;
    }

    /**
     * {@link EventTimeWatermarkGenerateMode} indicates the frequency at which event-time watermarks
     * are generated.
     */
    public enum EventTimeWatermarkGenerateMode {
        NO_WATERMARK,
        PERIODIC,
        PER_EVENT,
    }
}
