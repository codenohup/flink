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

package org.apache.flink.datastream.impl.extension.window.utils;

import org.apache.flink.datastream.api.extension.window.strategy.GlobalWindowStrategy;
import org.apache.flink.datastream.api.extension.window.strategy.SessionWindowStrategy;
import org.apache.flink.datastream.api.extension.window.strategy.SlidingTimeWindowStrategy;
import org.apache.flink.datastream.api.extension.window.strategy.TumblingTimeWindowStrategy;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.impl.extension.window.context.WindowTriggerContext;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class WindowUtil {
    public static WindowAssigner<?, ?> createWindowAssigner(WindowStrategy windowStrategy) {
        if (windowStrategy instanceof GlobalWindowStrategy) {
            return createGlobalWindowAssigner();
        } else if (windowStrategy instanceof TumblingTimeWindowStrategy) {
            return createTumblingTimeWindowAssigner((TumblingTimeWindowStrategy) windowStrategy);
        } else if (windowStrategy instanceof SlidingTimeWindowStrategy) {
            return createSlidingTimeWindowAssigner((SlidingTimeWindowStrategy) windowStrategy);
        } else if (windowStrategy instanceof SessionWindowStrategy) {
            return createSessionWindowAssigner((SessionWindowStrategy) windowStrategy);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported type of window strategy : " + windowStrategy.getClass());
        }
    }

    private static WindowAssigner<?, ?> createGlobalWindowAssigner() {
        return GlobalWindows.createWithEndOfStreamTrigger();
    }

    private static WindowAssigner<?, ?> createTumblingTimeWindowAssigner(
            TumblingTimeWindowStrategy windowStrategy) {
        switch (windowStrategy.getTimeType()) {
            case PROCESSING:
                return TumblingProcessingTimeWindows.of(
                        windowStrategy.getWindowSize(), windowStrategy.getAllowedLateness());
            case EVENT:
                return TumblingEventTimeWindows.of(
                        windowStrategy.getWindowSize(), windowStrategy.getAllowedLateness());
            default:
                throw new IllegalArgumentException(
                        "Unsupported time type : " + windowStrategy.getTimeType());
        }
    }

    private static WindowAssigner<?, ?> createSlidingTimeWindowAssigner(
            SlidingTimeWindowStrategy windowStrategy) {
        switch (windowStrategy.getTimeType()) {
            case PROCESSING:
                return SlidingProcessingTimeWindows.of(
                        windowStrategy.getWindowSize(),
                        windowStrategy.getWindowSlideInterval(),
                        windowStrategy.getAllowedLateness());
            case EVENT:
                return SlidingEventTimeWindows.of(
                        windowStrategy.getWindowSize(),
                        windowStrategy.getWindowSlideInterval(),
                        windowStrategy.getAllowedLateness());
            default:
                throw new IllegalArgumentException(
                        "Unsupported time type : " + windowStrategy.getTimeType());
        }
    }

    private static WindowAssigner<?, ?> createSessionWindowAssigner(
            SessionWindowStrategy windowStrategy) {
        switch (windowStrategy.getTimeType()) {
            case PROCESSING:
                return ProcessingTimeSessionWindows.withGap(windowStrategy.getSessionGap());
            case EVENT:
                return EventTimeSessionWindows.withGap(windowStrategy.getSessionGap());
            default:
                throw new IllegalArgumentException(
                        "Unsupported time type : " + windowStrategy.getTimeType());
        }
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
     * the given window.
     */
    public static <W extends Window> boolean isWindowLate(
            W window,
            WindowAssigner<?, W> windowAssigner,
            InternalTimerService<W> internalTimerService,
            long allowedLateness) {
        return (windowAssigner.isEventTime()
                && (cleanupTime(window, windowAssigner, allowedLateness)
                        <= internalTimerService.currentWatermark()));
    }

    /**
     * Decide if a record is currently late, based on current watermark and allowed lateness.
     *
     * @param element The element to check
     * @return The element for which should be considered when sideoutputs
     */
    public static boolean isElementLate(
            StreamRecord<?> element,
            WindowAssigner<?, ?> windowAssigner,
            long allowedLateness,
            InternalTimerService<?> internalTimerService) {
        return (windowAssigner.isEventTime())
                && (element.getTimestamp() + allowedLateness
                        <= internalTimerService.currentWatermark());
    }

    /**
     * Deletes the cleanup timer set for the contents of the provided window.
     *
     * @param window the window whose state to discard
     */
    public static <W extends Window> void deleteCleanupTimer(
            W window,
            WindowAssigner<?, ?> windowAssigner,
            WindowTriggerContext<?, ?, W> triggerContext,
            long allowedLateness) {
        long cleanupTime = cleanupTime(window, windowAssigner, allowedLateness);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.deleteEventTimeTimer(cleanupTime);
        } else {
            triggerContext.deleteProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Registers a timer to cleanup the content of the window.
     *
     * @param window the window whose state to discard
     */
    public static <W extends Window> void registerCleanupTimer(
            W window,
            WindowAssigner<?, ?> windowAssigner,
            WindowTriggerContext<?, ?, W> triggerContext,
            long allowedLateness) {
        long cleanupTime = cleanupTime(window, windowAssigner, allowedLateness);
        if (cleanupTime == Long.MAX_VALUE) {
            // don't set a GC timer for "end of time"
            return;
        }

        if (windowAssigner.isEventTime()) {
            triggerContext.registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext.registerProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Returns the cleanup time for a window, which is {@code window.maxTimestamp +
     * allowedLateness}. In case this leads to a value greater than {@link Long#MAX_VALUE} then a
     * cleanup time of {@link Long#MAX_VALUE} is returned.
     *
     * @param window the window whose cleanup time we are computing.
     */
    private static <W extends Window> long cleanupTime(
            W window, WindowAssigner<?, ?> windowAssigner, long allowedLateness) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }

    /** Returns {@code true} if the given time is the cleanup time for the given window. */
    public static <W extends Window> boolean isCleanupTime(
            W window, long time, WindowAssigner<?, ?> windowAssigner, long allowedLateness) {
        return time == cleanupTime(window, windowAssigner, allowedLateness);
    }
}
