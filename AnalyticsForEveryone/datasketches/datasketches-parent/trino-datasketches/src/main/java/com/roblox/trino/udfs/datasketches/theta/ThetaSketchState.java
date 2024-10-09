package com.roblox.trino.udfs.datasketches.theta;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateSerializerClass = ThetaSketchStateSerializer.class,
        stateFactoryClass = ThetaSketchStateFactory.class)
public interface ThetaSketchState
        extends AccumulatorState
{
    ThetaSketchProxy getThetaSketchProxy();

    void setThetaSketchProxy(ThetaSketchProxy value);
}
