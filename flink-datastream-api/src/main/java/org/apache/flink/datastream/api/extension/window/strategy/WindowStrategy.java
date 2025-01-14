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

package org.apache.flink.datastream.api.extension.window.strategy;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.time.Duration;

/** The WindowStrategy defines how to generate Window in the stream. */
@Experimental
public class WindowStrategy implements Serializable {

    public static final TimeType PROCESSING_TIME = TimeType.PROCESSING;
    public static final TimeType EVENT_TIME = TimeType.EVENT;

    /** The types of time used in window operations. */
    public enum TimeType {
        PROCESSING,
        EVENT
    }

    // ============== global window ================

    /** Creates a global window strategy. */
    public static WindowStrategy global() {
        return new GlobalWindowStrategy();
    }

    // ============== tumbling time window ================

    /**
     * Create a tumbling time window strategy and set the window size, the {@code timeType} of
     * Window will be set to EVENT, the {@code #allowedLateness} of Window will be set to 0.
     */
    public static WindowStrategy tumbling(Duration windowSize) {
        return new TumblingTimeWindowStrategy(windowSize);
    }

    /**
     * Create a tumbling time window strategy and set the window size and time type, the {@code
     * #allowedLateness} of Window will be set to 0.
     */
    public static WindowStrategy tumbling(Duration windowSize, TimeType timeType) {
        return new TumblingTimeWindowStrategy(windowSize, timeType);
    }

    /**
     * Create a tumbling time window strategy and set the window size, time type and allowed
     * lateness.
     */
    public static WindowStrategy tumbling(
            Duration windowSize, TimeType timeType, Duration allowedLateness) {
        return new TumblingTimeWindowStrategy(windowSize, timeType, allowedLateness);
    }

    // ============== sliding time window ================

    /**
     * Create a sliding time window strategy and set the window size and slide interval, the {@code
     * timeType} of Window will be set to EVENT, the {@code #allowedLateness} of Window will be set
     * to 0.
     */
    public static WindowStrategy sliding(Duration windowSize, Duration windowSlideInterval) {
        return new SlidingTimeWindowStrategy(windowSize, windowSlideInterval);
    }

    /**
     * Create a sliding time window strategy and set the window size, slide interval and time type,
     * the {@code #allowedLateness} of Window will be set to 0.
     */
    public static WindowStrategy sliding(
            Duration windowSize, Duration windowSlideInterval, TimeType timeType) {
        return new SlidingTimeWindowStrategy(windowSize, windowSlideInterval, timeType);
    }

    /**
     * Create a sliding time window strategy and set the window size, slide interval, time type and
     * allowed lateness.
     */
    public static WindowStrategy sliding(
            Duration windowSize,
            Duration windowSlideInterval,
            TimeType timeType,
            Duration allowedLateness) {
        return new SlidingTimeWindowStrategy(
                windowSize, windowSlideInterval, timeType, allowedLateness);
    }

    // ============== session window ================

    /**
     * Create a session window strategy and set the session gap, the {@code timeType} of Window will
     * be set to EVENT, the {@code #allowedLateness} of Window will be set to 0.
     */
    public static WindowStrategy session(Duration sessionGap) {
        return new SessionWindowStrategy(sessionGap);
    }

    /**
     * Create a session window strategy and set the session gap and time type, the default {@code
     * #allowedLateness} of Window will be set to 0.
     */
    public static WindowStrategy session(Duration sessionGap, TimeType timeType) {
        return new SessionWindowStrategy(sessionGap, timeType);
    }

    /** Create a session window strategy and set the session gap, time type and allowed lateness. */
    public static WindowStrategy session(
            Duration sessionGap, TimeType timeType, Duration allowedLateness) {
        return new SessionWindowStrategy(sessionGap, timeType, allowedLateness);
    }
}
