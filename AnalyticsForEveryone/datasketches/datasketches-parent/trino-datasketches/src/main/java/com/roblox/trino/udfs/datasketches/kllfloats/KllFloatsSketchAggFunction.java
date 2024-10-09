package com.roblox.trino.udfs.datasketches.kllfloats;

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarbinaryType;

@Description("Create a kll floats sketch by aggregating values or kll floats sketches")
@AggregationFunction("kll_floats_sketch")
public class KllFloatsSketchAggFunction
{
    private KllFloatsSketchAggFunction() {}

    @InputFunction
    public static void inputDouble(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy());
        }
        state.getKllSketchProxy().put((float) value);
    }

    @InputFunction
    public static void inputBigint(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy());
        }
        state.getKllSketchProxy().put((float) value);
    }

    @InputFunction
    public static void inputReal(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.REAL) long value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy());
        }
        float floatValue = Float.intBitsToFloat((int) value);
        state.getKllSketchProxy().put(floatValue);
    }

    @InputFunction
    public static void inputSketch(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        KllFloatsSketchProxy otherSketch = new KllFloatsSketchProxy(value);
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy(otherSketch.getK()));
        }
        state.getKllSketchProxy().union(otherSketch);
    }

    @InputFunction
    public static void inputDoubleWithK(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy((int) k));
        }
        state.getKllSketchProxy().put((float) value);
    }

    @InputFunction
    public static void inputBigintWithK(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy((int) k));
        }
        state.getKllSketchProxy().put((float) value);
    }

    @InputFunction
    public static void inputRealWithK(
            KllFloatsSketchState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy((int) k));
        }
        float floatValue = Float.intBitsToFloat((int) value);
        state.getKllSketchProxy().put(floatValue);
    }

    @CombineFunction
    public static void combine(KllFloatsSketchState state, KllFloatsSketchState otherState)
    {
        KllFloatsSketchProxy otherSketch = otherState.getKllSketchProxy();
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy(otherSketch.getK()));
        }
        state.getKllSketchProxy().union(otherSketch);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(KllFloatsSketchState state, BlockBuilder out)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllFloatsSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getKllSketchProxy().serialize());
    }
}
