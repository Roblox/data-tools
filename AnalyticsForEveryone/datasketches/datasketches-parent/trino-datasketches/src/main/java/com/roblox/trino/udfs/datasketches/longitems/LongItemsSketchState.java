package com.roblox.trino.udfs.datasketches.longitems;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = LongItemsSketchStateSerializer.class,
        stateFactoryClass = LongItemsSketchStateFactory.class)
public interface LongItemsSketchState
        extends AccumulatorState
{
    LongItemsSketchProxy getItemsSketchProxy();

    void setItemsSketchProxy(LongItemsSketchProxy value);
}
