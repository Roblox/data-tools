package com.roblox.trino.udfs.datasketches.theta;

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

@Description("Create a theta sketch by aggregating values or theta sketches")
@AggregationFunction("theta_sketch")
public class ThetaSketchAggFunction
{
    private ThetaSketchAggFunction() {}

    private static ThetaSketchProxy getOrCreateSketchProxy(ThetaSketchState state, int k)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy(k));
        }
        return state.getThetaSketchProxy();
    }

    @InputFunction
    public static void inputString(
            ThetaSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy());
        }
        state.getThetaSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputDouble(
            ThetaSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy());
        }
        state.getThetaSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigint(
            ThetaSketchState state,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy());
        }
        state.getThetaSketchProxy().put(value);
    }

    @InputFunction
    public static void inputReal(
            ThetaSketchState state,
            @SqlType(StandardTypes.REAL) long value)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy());
        }
        float floatValue = Float.intBitsToFloat((int) value);
        state.getThetaSketchProxy().put(floatValue);
    }

    @InputFunction
    public static void inputSketch(
            ThetaSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        ThetaSketchProxy other = new ThetaSketchProxy(value, ThetaSketchProxy.DEFAULT_K);
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy(other.getK()));
        }
        state.getThetaSketchProxy().union(other);
    }

    @InputFunction
    public static void inputStringWithK(
            ThetaSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy((int) k));
        }
        state.getThetaSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputDoubleWithK(
            ThetaSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy((int) k));
        }
        state.getThetaSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigintWithK(
            ThetaSketchState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy((int) k));
        }
        state.getThetaSketchProxy().put(value);
    }

    @InputFunction
    public static void inputRealWithK(
            ThetaSketchState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy((int) k));
        }
        float floatValue = Float.intBitsToFloat((int) value);
        state.getThetaSketchProxy().put(floatValue);
    }

    @InputFunction
    public static void inputSketchWithK(
            ThetaSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy other = new ThetaSketchProxy(value, (int) k);
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy(other.getK()));
        }
        state.getThetaSketchProxy().union(other);
    }

    @CombineFunction
    public static void combine(ThetaSketchState state, ThetaSketchState otherState)
    {
        ThetaSketchProxy otherSketch = otherState.getThetaSketchProxy();
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy(otherSketch.getK()));
        }
        state.getThetaSketchProxy().union(otherSketch);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(ThetaSketchState state, BlockBuilder out)
    {
        if (state.getThetaSketchProxy() == null) {
            state.setThetaSketchProxy(new ThetaSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getThetaSketchProxy().serialize());
    }
}
