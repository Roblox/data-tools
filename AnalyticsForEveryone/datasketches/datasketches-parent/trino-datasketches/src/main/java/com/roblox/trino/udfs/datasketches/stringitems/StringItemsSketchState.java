package com.roblox.trino.udfs.datasketches.stringitems;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = StringItemsSketchStateSerializer.class,
        stateFactoryClass = StringItemsSketchStateFactory.class)
public interface StringItemsSketchState
        extends AccumulatorState
{
    StringItemsSketchProxy getItemsSketchProxy();

    void setItemsSketchProxy(StringItemsSketchProxy value);
}
