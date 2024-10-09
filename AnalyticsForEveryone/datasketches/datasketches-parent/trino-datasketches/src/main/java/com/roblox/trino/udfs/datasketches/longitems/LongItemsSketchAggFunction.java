package com.roblox.trino.udfs.datasketches.longitems;

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

@Description("Create a long items sketch by aggregating values or long items sketches")
@AggregationFunction("long_items_sketch")
public class LongItemsSketchAggFunction
{
    private LongItemsSketchAggFunction() {}

    private static void mergeSketchToState(LongItemsSketchState state, LongItemsSketchProxy otherSketch)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new LongItemsSketchProxy(otherSketch.getMaxMapSize()));
        }
        LongItemsSketchProxy sketch = state.getItemsSketchProxy();
        sketch.merge(otherSketch);
    }

    @InputFunction
    public static void inputLong(
            LongItemsSketchState state,
            @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new LongItemsSketchProxy());
        }
        state.getItemsSketchProxy().put(value);
    }

    @InputFunction
    public static void inputInteger(
            LongItemsSketchState state,
            @SqlType(StandardTypes.INTEGER) long value)
    {
        inputLong(state, value);
    }

    @InputFunction
    public static void inputLongWithSize(
            LongItemsSketchState state,
            @SqlType(StandardTypes.DOUBLE) long value,
            @SqlType(StandardTypes.BIGINT) long maxSize)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new LongItemsSketchProxy((int) maxSize));
        }
        state.getItemsSketchProxy().put(value);
    }

    @InputFunction
    public static void inputIntegerWithSize(
            LongItemsSketchState state,
            @SqlType(StandardTypes.INTEGER) long value,
            @SqlType(StandardTypes.BIGINT) long maxSize)
    {
        inputLongWithSize(state, value, maxSize);
    }

    @InputFunction
    public static void inputSketch(
            LongItemsSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        mergeSketchToState(state, new LongItemsSketchProxy(value));
    }

    @CombineFunction
    public static void combine(LongItemsSketchState state, LongItemsSketchState otherState)
    {
        mergeSketchToState(state, otherState.getItemsSketchProxy());
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(LongItemsSketchState state, BlockBuilder out)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new LongItemsSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getItemsSketchProxy().serialize());
    }
}
