package org.apache.flink.streaming.runtime.watermark;

import org.apache.flink.api.common.watermark.WatermarkCombinationFunction;
import org.apache.flink.api.common.watermark.WatermarkCombinationPolicy;
import org.apache.flink.api.common.watermark.WatermarkHandlingStrategy;

public class InternalDeclaredWatermarks {

    public static final InternalLongWatermarkDeclaration INTERNAL_EVENT_TIME_WATERMARK_DECLARATION =
            new InternalLongWatermarkDeclaration(
                    "INTERNAL_EVENT_TIME",
                    new WatermarkCombinationPolicy(
                            WatermarkCombinationFunction.NumericWatermarkCombinationFunction.MIN,
                            true),
                    WatermarkHandlingStrategy.FORWARD);
}
