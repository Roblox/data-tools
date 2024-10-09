package com.roblox.trino.udfs.datasketches.doubleitems;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = DoubleItemsSketchStateSerializer.class,
        stateFactoryClass = DoubleItemsSketchStateFactory.class)
public interface DoubleItemsSketchState
        extends AccumulatorState
{
    DoubleItemsSketchProxy getItemsSketchProxy();

    void setItemsSketchProxy(DoubleItemsSketchProxy value);
}
