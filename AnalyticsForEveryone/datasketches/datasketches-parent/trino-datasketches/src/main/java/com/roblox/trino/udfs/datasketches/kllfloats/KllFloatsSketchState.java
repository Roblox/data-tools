package com.roblox.trino.udfs.datasketches.kllfloats;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = KllFloatsSketchStateSerializer.class,
        stateFactoryClass = KllFloatsSketchStateFactory.class)
public interface KllFloatsSketchState
        extends AccumulatorState
{
    KllFloatsSketchProxy getKllSketchProxy();

    void setKllSketchProxy(KllFloatsSketchProxy value);
}
