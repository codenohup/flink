package org.apache.flink.datastream.impl.watermark;

import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.EventTimeExtractor;
import org.apache.flink.streaming.runtime.watermark.InternalDeclaredWatermarks;

import java.util.Collection;
import java.util.Collections;

public class ExtractEventTimeProcessFunction<IN> implements OneInputStreamProcessFunction<IN, IN> {

    private final EventTimeExtractor<IN> assigner;

    public ExtractEventTimeProcessFunction(EventTimeExtractor<IN> assigner) {
        this.assigner = assigner;
    }

    @Override
    public void processRecord(IN record, Collector<IN> output, PartitionedContext ctx)
            throws Exception {
        long eventTimeInMS = assigner.extractTimestamp(record);
        LongWatermark watermark =
                InternalDeclaredWatermarks.INTERNAL_EVENT_TIME_WATERMARK_DECLARATION.newWatermark(
                        eventTimeInMS);
        // todo: periodic emit rather than continuous
        ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
        output.collect(record);
    }

    @Override
    public Collection<? extends WatermarkDeclaration> watermarkDeclarations() {
        return Collections.singletonList(
                InternalDeclaredWatermarks.INTERNAL_EVENT_TIME_WATERMARK_DECLARATION);
    }
}
