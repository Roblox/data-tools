package com.roblox.trino.udfs.datasketches.klldoubles;

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

@Description("Create a kll doubles sketch by aggregating values or kll doubles sketches")
@AggregationFunction("kll_doubles_sketch")
public class KllDoublesSketchAggFunction
{
    private KllDoublesSketchAggFunction() {}

    @InputFunction
    public static void inputDouble(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy());
        }
        state.getKllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigint(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy());
        }
        state.getKllSketchProxy().put((double) value);
    }

    @InputFunction
    public static void inputReal(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.REAL) long value)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy());
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getKllSketchProxy().put(doubleValue);
    }

    @InputFunction
    public static void inputDoubleWithK(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy((int) k));
        }
        state.getKllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigintWithK(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy((int) k));
        }
        state.getKllSketchProxy().put((double) value);
    }

    @InputFunction
    public static void inputRealWithK(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy((int) k));
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getKllSketchProxy().put(doubleValue);
    }

    @InputFunction
    public static void inputSketch(
            KllDoublesSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        KllDoublesSketchProxy otherSketch = new KllDoublesSketchProxy(value);
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy(otherSketch.getK()));
        }
        state.getKllSketchProxy().union(otherSketch);
    }

    @CombineFunction
    public static void combine(KllDoublesSketchState state, KllDoublesSketchState otherState)
    {
        KllDoublesSketchProxy otherSketch = otherState.getKllSketchProxy();
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy(otherSketch.getK()));
        }
        state.getKllSketchProxy().union(otherSketch);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(KllDoublesSketchState state, BlockBuilder out)
    {
        if (state.getKllSketchProxy() == null) {
            state.setKllSketchProxy(new KllDoublesSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getKllSketchProxy().serialize());
    }
}
