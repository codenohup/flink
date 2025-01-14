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

package org.apache.flink.datastream.impl.extension.window;

import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonNroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.utils.TaggedUnion;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalOneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowExtensionImpl {

    public static <IN, OUT, W extends Window> OneInputStreamProcessFunction<IN, OUT> process(
            OneInputWindowStreamProcessFunction<IN, OUT> processFunction,
            WindowAssigner<IN, W> windowAssigner,
            Trigger<IN, W> trigger) {
        return new InternalOneInputWindowStreamProcessFunction<>(
                processFunction, windowAssigner, trigger);
    }

    public static <IN, OUT, W extends Window>
            OneInputStreamProcessFunction<IN, OUT> processWithoutIterator(
                    OneInputWindowStreamProcessFunction<IN, OUT> processFunction,
                    WindowAssigner<IN, W> windowAssigner,
                    Trigger<IN, W> trigger) {
        return new InternalOneInputWindowStreamProcessFunction<>(
                processFunction, windowAssigner, trigger);
    }

    public static <IN1, IN2, OUT, W extends Window>
            TwoInputNonBroadcastStreamProcessFunction<IN1, IN2, OUT> process(
                    TwoInputNonNroadcastWindowStreamProcessFunction<IN1, IN2, OUT> processFunction,
                    WindowAssigner<TaggedUnion<IN1, IN2>, W> windowAssigner,
                    Trigger<TaggedUnion<IN1, IN2>, W> trigger) {
        return new InternalTwoInputWindowStreamProcessFunction<>(
                processFunction, windowAssigner, trigger);
    }
}
