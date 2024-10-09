package com.roblox.trino.udfs.datasketches.hll;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = HllSketchStateSerializer.class,
        stateFactoryClass = HllSketchStateFactory.class)
public interface HllSketchState
        extends AccumulatorState
{
    HllSketchProxy getHllSketchProxy();

    void setHllSketchProxy(HllSketchProxy value);
}
