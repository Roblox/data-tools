package com.roblox.trino.udfs.datasketches.stringitems;

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

@Description("Create a string items sketch by aggregating values or string items sketches")
@AggregationFunction("string_items_sketch")
public class StringItemsSketchAggFunction
{
    private StringItemsSketchAggFunction() {}

    private static void mergeSketchToState(StringItemsSketchState state, StringItemsSketchProxy otherSketch)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new StringItemsSketchProxy(otherSketch.getMaxMapSize()));
        }
        StringItemsSketchProxy sketch = state.getItemsSketchProxy();
        sketch.merge(otherSketch);
    }

    @InputFunction
    public static void inputString(
            StringItemsSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new StringItemsSketchProxy());
        }
        state.getItemsSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputStringWithSize(
            StringItemsSketchState state,
            @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.BIGINT) long maxSize)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new StringItemsSketchProxy((int) maxSize));
        }
        state.getItemsSketchProxy().put(value.toStringUtf8());
    }

    @InputFunction
    public static void inputSketch(
            StringItemsSketchState state,
            @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        mergeSketchToState(state, new StringItemsSketchProxy(value));
    }

    @CombineFunction
    public static void combine(StringItemsSketchState state, StringItemsSketchState otherState)
    {
        mergeSketchToState(state, otherState.getItemsSketchProxy());
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(StringItemsSketchState state, BlockBuilder out)
    {
        if (state.getItemsSketchProxy() == null) {
            state.setItemsSketchProxy(new StringItemsSketchProxy());
        }
        VarbinaryType.VARBINARY.writeSlice(out, state.getItemsSketchProxy().serialize());
    }
}
