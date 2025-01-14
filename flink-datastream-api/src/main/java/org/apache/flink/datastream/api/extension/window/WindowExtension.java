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

package org.apache.flink.datastream.api.extension.window;

/** Utils class for build window. */
public class WindowExtension {

    private static final Class<?> INSTANCE;

    static {
        try {
            INSTANCE =
                    Class.forName(
                            "org.apache.flink.datastream.impl.extension.window.WindowExtensionImpl");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Please ensure that flink-process-function in your class path");
        }
    }

    //
    //    public static <IN, W extends Window> WindowBuilder<IN, W> builder(
    //            WindowAssigner<IN, W> windowAssigner) {
    //        return new WindowBuilder<>(windowAssigner);
    //    }
    //
    //    public static <IN1, IN2, W extends Window> TwoInputWindowBuilder<IN1, IN2, W>
    // twoInputBuilder(
    //            WindowAssigner<TaggedUnion<IN1, IN2>, W> windowAssigner) {
    //        return new TwoInputWindowBuilder<>(windowAssigner);
    //    }

    //    public static <IN, OUT, ACC, W extends Window> OneInputStreamProcessFunction<IN, OUT>
    // apply(
    //            WindowBuilder<IN, W> window,
    //            WindowProcessFunctionWithAggregation<IN, ACC, OUT, W> windowProcessFunction) {
    //        try {
    ////            return (OneInputStreamProcessFunction<IN, OUT>)
    ////                    INSTANCE.getMethod(
    ////                                    "process",
    ////                                    WindowProcessFunctionWithAggregation.class,
    ////                                    WindowAssigner.class,
    ////                                    Trigger.class)
    ////                            .invoke(null, windowProcessFunction, window.assigner,
    // window.trigger);
    //            // TODO
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }

    //    public static class TwoInputWindowBuilder<IN1, IN2, W extends Window> {
    //        private final WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner;
    //
    //        private Trigger<TaggedUnion<IN1, IN2>, W> trigger;
    //
    //        public TwoInputWindowBuilder(WindowAssigner<TaggedUnion<IN1, IN2>, W> assigner) {
    //            this.assigner = checkNotNull(assigner);
    //            this.trigger = assigner.getDefaultTrigger();
    //        }
    //
    //        public TwoInputWindowBuilder<IN1, IN2, W> withTrigger(
    //                Trigger<TaggedUnion<IN1, IN2>, W> trigger) {
    //            this.trigger = checkNotNull(trigger);
    //            return this;
    //        }
    //
    //        public WindowAssigner<TaggedUnion<IN1, IN2>, W> getAssigner() {
    //            return assigner;
    //        }
    //
    //        public Trigger<TaggedUnion<IN1, IN2>, W> getTrigger() {
    //            return trigger;
    //        }
    //    }

    //    public static <IN1, IN2, OUT, W extends Window>
    //            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> apply(
    //                    WindowBuilder<?, W> window,
    //                    TwoInputWindowStreamProcessFunction<IN1, IN2, OUT>
    //                            windowProcessFunction) {
    //        try {
    //            return (TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>)
    //                    INSTANCE.getMethod(
    //                                    "process",
    //                                    TwoInputWindowStreamProcessFunction.class,
    //                                    WindowAssigner.class,
    //                                    Trigger.class)
    //                            .invoke(
    //                                    null,
    //                                    windowProcessFunction,
    //                                    window.getAssigner(),
    //                                    window.getTrigger());
    //            // TOOD:
    //            return null;
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }

    @SuppressWarnings("unchecked")
    //    public static <IN1, IN2, OUT, W extends Window>
    //            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> apply(
    //                    TwoInputWindowBuilder<IN1, IN2, W> window,
    //                    TwoInputWindowStreamProcessFunction<IN1, IN2, OUT>
    //                            windowProcessFunction) {
    //        try {
    //            return (TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT>)
    //                    INSTANCE.getMethod(
    //                                    "process",
    //                                    TwoInputWindowStreamProcessFunction.class,
    //                                    WindowAssigner.class,
    //                                    Trigger.class)
    //                            .invoke(null, windowProcessFunction, window.assigner,
    // window.trigger);
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }
    //
    //    @SuppressWarnings("unchecked")
    //    public static <IN, OUT, W extends Window> OneInputStreamProcessFunction<IN, OUT> apply(
    //            WindowBuilder<IN, W> window,
    //            OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction) {
    //        try {
    //            return (OneInputStreamProcessFunction<IN, OUT>)
    //                    INSTANCE.getMethod(
    //                                    "process",
    //                                    OneInputWindowStreamProcessFunction.class,
    //                                    WindowAssigner.class,
    //                                    Trigger.class)
    //                            .invoke(null, windowProcessFunction, window.assigner,
    // window.trigger);
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }

    //    public static <IN, OUT, W extends Window> OneInputStreamProcessFunction<IN, OUT> apply(
    //            WindowStrategy windowStrategy,
    //            OneInputWindowStreamProcessFunction<IN, OUT> windowProcessFunction) {
    //        if (windowStrategy instanceof WindowStrategy.TimeWindowStrategy) {
    //            WindowStrategy.TimeWindowStrategy timeWindowStrategy =
    //                    (WindowStrategy.TimeWindowStrategy) windowStrategy;
    //            WindowBuilder<Object, TimeWindow> windowBuilder;
    //            TimeWindows.TimeType timeType =
    //                    timeWindowStrategy.timeType == WindowStrategy.TimeType.PROCESSING
    //                            ? TimeWindows.TimeType.PROCESSING
    //                            : TimeWindows.TimeType.EVENT;
    //            if (timeWindowStrategy.timeWindowType == WindowStrategy.TimeWindowType.TUMBLING) {
    //                windowBuilder = TimeWindows.ofTumbling(timeWindowStrategy.windowSize,
    // timeType);
    //            } else {
    //                windowBuilder =
    //                        TimeWindows.ofSliding(
    //                                timeWindowStrategy.windowSize,
    //                                timeWindowStrategy.windowSlideInterval,
    //                                timeType);
    //            }
    //            try {
    //                return (OneInputStreamProcessFunction<IN, OUT>)
    //                        INSTANCE.getMethod(
    //                                        "processWithoutIterator",
    //                                        OneInputWindowStreamProcessFunction.class,
    //                                        WindowAssigner.class,
    //                                        Trigger.class)
    //                                .invoke(
    //                                        null,
    //                                        windowProcessFunction,
    //                                        windowBuilder.assigner,
    //                                        windowBuilder.trigger);
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            }
    //        }
    //        return null;
    //    }
    //
    //    @SuppressWarnings("unchecked")
    //    public static <IN, W extends Window> OneInputStreamProcessFunction<IN, IN> apply(
    //            WindowBuilder<IN, W> window, ReduceFunction<IN> reduceFunction) {
    //        try {
    //            return (OneInputStreamProcessFunction<IN, IN>)
    //                    INSTANCE.getMethod(
    //                                    "reduce",
    //                                    ReduceFunction.class,
    //                                    WindowAssigner.class,
    //                                    Trigger.class)
    //                            .invoke(null, reduceFunction, window.assigner, window.trigger);
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }
    //
    //    /**
    //     * Pre-aggregate elements but provide more context than {@link #apply(WindowBuilder,
    //     * ReduceFunction)}.
    //     */
    //    @SuppressWarnings("unchecked")
    //    public static <IN, W extends Window> OneInputStreamProcessFunction<IN, IN> apply(
    //            WindowBuilder<IN, W> window,
    //            ReduceFunction<IN> reduceFunction,
    //            OneInputWindowStreamProcessFunction<IN, IN> windowProcessFunction) {
    //        try {
    //            return (OneInputStreamProcessFunction<IN, IN>)
    //                    INSTANCE.getMethod(
    //                                    "reduce",
    //                                    ReduceFunction.class,
    //                                    OneInputWindowStreamProcessFunction.class,
    //                                    WindowAssigner.class,
    //                                    Trigger.class)
    //                            .invoke(
    //                                    null,
    //                                    reduceFunction,
    //                                    windowProcessFunction,
    //                                    window.assigner,
    //                                    window.trigger);
    //        } catch (Exception e) {
    //            throw new RuntimeException(e);
    //        }
    //    }

    public static class GlobalWindows {
        private static final Class<?> INSTANCE;

        static {
            try {
                INSTANCE =
                        Class.forName(
                                "org.apache.flink.datastream.impl.extension.window.WindowExtensionImpl$GlobalWindowsImpl");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-process-function in your class path");
            }
        }

        //        @SuppressWarnings("unchecked")
        //        public static <IN> WindowBuilder<IN, Window> create() {
        //            try {
        //                return new WindowBuilder<>(
        //                        (WindowAssigner<IN, Window>)
        // INSTANCE.getMethod("create").invoke(null));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }
        //
        //        @SuppressWarnings("unchecked")
        //        public static <IN1, IN2> TwoInputWindowBuilder<IN1, IN2, Window> createTwoInput()
        // {
        //            try {
        //                return new TwoInputWindowBuilder<>(
        //                        (WindowAssigner<TaggedUnion<IN1, IN2>, Window>)
        //                                INSTANCE.getMethod("create").invoke(null));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }
    }

    public static class TimeWindows {
        private static final Class<?> INSTANCE;

        static {
            try {
                INSTANCE =
                        Class.forName(
                                "org.apache.flink.datastream.impl.extension.window.WindowExtensionImpl$TimeWindowsImpl");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        "Please ensure that flink-process-function in your class path");
            }
        }

        //        @SuppressWarnings("unchecked")
        //        public static <IN> WindowBuilder<IN, TimeWindow> ofSliding(
        //                Duration size, Duration slide, TimeType timeType) {
        //            try {
        //                return new WindowBuilder<>(
        //                        (WindowAssigner<IN, TimeWindow>)
        //                                INSTANCE.getMethod(
        //                                                "ofSliding",
        //                                                Duration.class,
        //                                                Duration.class,
        //                                                TimeType.class)
        //                                        .invoke(null, size, slide, timeType));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }

        //        @SuppressWarnings("unchecked")
        //        public static <IN> WindowBuilder<IN, TimeWindow> ofTumbling(
        //                Duration size, TimeType timeType) {
        //            try {
        //                return new WindowBuilder<>(
        //                        (WindowAssigner<IN, TimeWindow>)
        //                                INSTANCE.getMethod("ofTumbling", Duration.class,
        // TimeType.class)
        //                                        .invoke(null, size, timeType));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }
        //
        //        @SuppressWarnings("unchecked")
        //        public static <IN1, IN2> TwoInputWindowBuilder<IN1, IN2, Window>
        // ofTwoInputTumbling(
        //                Duration size, TimeType timeType) {
        //            try {
        //                return new TwoInputWindowBuilder<>(
        //                        (WindowAssigner<TaggedUnion<IN1, IN2>, Window>)
        //                                INSTANCE.getMethod("ofTumbling", Duration.class,
        // TimeType.class)
        //                                        .invoke(null, size, timeType));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }

        //        @SuppressWarnings("unchecked")
        //        public static <IN1, IN2> TwoInputWindowBuilder<IN1, IN2, Window>
        // ofTwoInputSliding(
        //                Duration size, Duration slide, TimeType timeType) {
        //            try {
        //                return new TwoInputWindowBuilder<>(
        //                        (WindowAssigner<TaggedUnion<IN1, IN2>, Window>)
        //                                INSTANCE.getMethod(
        //                                                "ofSliding",
        //                                                Duration.class,
        //                                                Duration.class,
        //                                                TimeType.class)
        //                                        .invoke(null, size, slide, timeType));
        //            } catch (Exception e) {
        //                throw new RuntimeException(e);
        //            }
        //        }

        public enum TimeType {
            PROCESSING,
            EVENT
        }
    }
}
