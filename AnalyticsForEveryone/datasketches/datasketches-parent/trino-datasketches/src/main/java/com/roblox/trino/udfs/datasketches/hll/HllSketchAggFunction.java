package com.roblox.trino.udfs.datasketches.hll;

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
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;

@Description("Create an hll sketch by aggregating values or hll sketches")
@AggregationFunction("hll_sketch")
public class HllSketchAggFunction
{
    private HllSketchAggFunction() {}

    private static void mergeSketchToState(HllSketchState state, HllSketchProxy otherSketch)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy(otherSketch.getLgK()));
        }
        HllSketchProxy sketch = state.getHllSketchProxy();

        Union union = new Union(sketch.getLgK());
        union.update(sketch.getSketch());
        union.update(otherSketch.getSketch());
        HllSketch unionResult = union.getResult(sketch.getSketch().getTgtHllType());
        state.setHllSketchProxy(new HllSketchProxy(unionResult));
    }

    @InputFunction
    public static void inputString(
            HllSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy());
        }
        state.getHllSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputDouble(
            HllSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy());
        }
        state.getHllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigint(
            HllSketchState state,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy());
        }
        state.getHllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputReal(
            HllSketchState state,
            @SqlType(StandardTypes.REAL) long value)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy());
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getHllSketchProxy().put(doubleValue);
    }

    @InputFunction
    public static void inputSketch(
            HllSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        mergeSketchToState(state, new HllSketchProxy(value));
    }

    @InputFunction
    public static void inputStringWithLgK(
            HllSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.BIGINT) long lgK)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy((int) lgK));
        }
        state.getHllSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputDoubleWithLgK(
            HllSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.BIGINT) long lgK)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy((int) lgK));
        }
        state.getHllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputBigintWithLgK(
            HllSketchState state,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType(StandardTypes.BIGINT) long lgK)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy((int) lgK));
        }
        state.getHllSketchProxy().put(value);
    }

    @InputFunction
    public static void inputRealWithLgK(
            HllSketchState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long lgK)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy((int) lgK));
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getHllSketchProxy().put(doubleValue);
    }

    @CombineFunction
    public static void combine(HllSketchState state, HllSketchState otherState)
    {
        mergeSketchToState(state, otherState.getHllSketchProxy());
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(HllSketchState state, BlockBuilder out)
    {
        if (state.getHllSketchProxy() == null) {
            state.setHllSketchProxy(new HllSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getHllSketchProxy().serialize());
    }
}
