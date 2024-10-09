package com.roblox.trino.udfs.datasketches.doubleitems;

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

@Description("Create a double items sketch by aggregating values or double items sketches")
@AggregationFunction("double_items_sketch")
public class DoubleItemsSketchAggFunction
{
    private DoubleItemsSketchAggFunction() {}

    private static void mergeSketchToState(DoubleItemsSketchState state, DoubleItemsSketchProxy otherSketch)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy(otherSketch.getMaxMapSize()));
        }
        DoubleItemsSketchProxy sketch = state.getItemsSketchProxy();
        sketch.merge(otherSketch);
    }

    @InputFunction
    public static void inputDouble(
            DoubleItemsSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy());
        }
        state.getItemsSketchProxy().put(value);
    }

    @InputFunction
    public static void inputReal(
            DoubleItemsSketchState state,
            @SqlType(StandardTypes.REAL) long value)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy());
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getItemsSketchProxy().put(doubleValue);
    }

    @InputFunction
    public static void inputDoubleWithSize(
            DoubleItemsSketchState state,
            @SqlType(StandardTypes.DOUBLE) double value,
            @SqlType(StandardTypes.BIGINT) long maxSize)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy((int) maxSize));
        }
        state.getItemsSketchProxy().put(value);
    }

    @InputFunction
    public static void inputRealWithSize(
            DoubleItemsSketchState state,
            @SqlType(StandardTypes.REAL) long value,
            @SqlType(StandardTypes.BIGINT) long maxSize)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy((int) maxSize));
        }
        double doubleValue = Float.intBitsToFloat((int) value);
        state.getItemsSketchProxy().put(doubleValue);
    }

    @InputFunction
    public static void inputSketch(
            DoubleItemsSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        mergeSketchToState(state, new DoubleItemsSketchProxy(value));
    }

    @CombineFunction
    public static void combine(DoubleItemsSketchState state, DoubleItemsSketchState otherState)
    {
        mergeSketchToState(state, otherState.getItemsSketchProxy());
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(DoubleItemsSketchState state, BlockBuilder out)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new DoubleItemsSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getItemsSketchProxy().serialize());
    }
}
