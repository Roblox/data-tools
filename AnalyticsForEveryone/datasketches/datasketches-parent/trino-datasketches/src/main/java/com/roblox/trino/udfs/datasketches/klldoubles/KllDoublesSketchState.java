package com.roblox.trino.udfs.datasketches.klldoubles;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = KllDoublesSketchStateSerializer.class,
        stateFactoryClass = KllDoublesSketchStateFactory.class)
public interface KllDoublesSketchState
        extends AccumulatorState
{
    KllDoublesSketchProxy getKllSketchProxy();

    void setKllSketchProxy(KllDoublesSketchProxy value);
}
