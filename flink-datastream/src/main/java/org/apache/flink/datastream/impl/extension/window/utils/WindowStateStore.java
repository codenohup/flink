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

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.runtime.state.v2.adaptor.ListStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.MapStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.ValueStateAdaptor;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class WindowStateStore<K, W extends Window> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowStateStore.class);

    private final WindowProcessFunction windowProcessFunction;

    private final AbstractAsyncStateStreamOperator<?> operator;

    private final TypeSerializer<W> windowSerializer;

    public WindowStateStore(
            WindowProcessFunction windowProcessFunction,
            AbstractAsyncStateStreamOperator<?> operator,
            TypeSerializer<W> windowSerializer) {
        this.windowProcessFunction = windowProcessFunction;
        this.operator = operator;
        this.windowSerializer = windowSerializer;
    }

    private boolean isStateDeclared(StateDeclaration stateDeclaration) {
        if (!windowProcessFunction.useWindowStates().contains(stateDeclaration)) {
            LOG.warn(
                    "Fail to get window state for "
                            + stateDeclaration.getName()
                            + ", please declare the used state in the `useWindowStates` method first.");
            return false;
        }
        return true;
    }

    private boolean stateRedistributionModeIsNone(StateDeclaration stateDeclaration) {
        StateDeclaration.RedistributionMode redistributionMode =
                stateDeclaration.getRedistributionMode();
        return redistributionMode == StateDeclaration.RedistributionMode.NONE;
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<ListState<T>> getWindowState(
            ListStateDeclaration<T> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ListStateDescriptor<T> stateDescriptor =
                new ListStateDescriptor<T>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        try {
            ListStateAdaptor<K, W, T> state =
                    (ListStateAdaptor<K, W, T>)
                            operator.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
            MapStateDeclaration<KEY, V> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        MapStateDescriptor<KEY, V> stateDescriptor =
                new MapStateDescriptor<KEY, V>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getKeyTypeDescriptor().getTypeClass()),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getValueTypeDescriptor().getTypeClass()));

        try {
            MapStateAdaptor<K, W, KEY, V> state =
                    (MapStateAdaptor<K, W, KEY, V>)
                            operator.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public <T> Optional<ValueState<T>> getWindowState(
            ValueStateDeclaration<T> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        //        if (stateRedistributionModeIsNone(stateDeclaration)) {
        //            throw new UnsupportedOperationException(
        //                    "RedistributionMode "
        //                            + stateDeclaration.getRedistributionMode().name()
        //                            + " is not supported for window state.");
        //        }

        ValueStateDescriptor<T> stateDescriptor =
                new ValueStateDescriptor<T>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        try {
            ValueStateAdaptor<K, W, T> state =
                    (ValueStateAdaptor<K, W, T>)
                            operator.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
