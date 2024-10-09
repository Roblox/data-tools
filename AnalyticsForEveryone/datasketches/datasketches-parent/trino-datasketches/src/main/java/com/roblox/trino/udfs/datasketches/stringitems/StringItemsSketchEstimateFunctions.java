package com.roblox.trino.udfs.datasketches.stringitems;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.VarcharType;

import static io.airlift.slice.Slices.utf8Slice;

public class StringItemsSketchEstimateFunctions
{
    private StringItemsSketchEstimateFunctions() {}

    @Description("Estimate the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchEstimate(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.VARCHAR) Slice itemSlice)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        String item = itemSlice.toStringUtf8();
        return sketch.getEstimate(item);
    }

    @Description("Estimate the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate")
    @SqlType("array(bigint)")
    public static Block itemsSketchEstimateArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(varchar)") Block itemsBlock)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            Slice itemSlice = itemsBlock.getSlice(i, 0, itemsBlock.getSliceLength(i));
            String item = itemSlice.toStringUtf8();
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getEstimate(item));
        }
        return blockBuilder.build();
    }

    @Description("Get an upper bound on the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.VARCHAR) Slice itemSlice)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        String item = itemSlice.toStringUtf8();
        return sketch.getUpperBound(item);
    }

    @Description("Get an upper bound on the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate_ub")
    @SqlType("array(bigint)")
    public static Block itemsSketchUpperBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(varchar)") Block itemsBlock)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            Slice itemSlice = itemsBlock.getSlice(i, 0, itemsBlock.getSliceLength(i));
            String item = itemSlice.toStringUtf8();
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getUpperBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get a lower bound on the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.VARCHAR) Slice itemSlice)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        String item = itemSlice.toStringUtf8();
        return sketch.getLowerBound(item);
    }

    @Description("Get a lower bound on the frequency of an item in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_estimate_lb")
    @SqlType("array(bigint)")
    public static Block itemsSketchLowerBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(varchar)") Block itemsBlock)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            Slice itemSlice = itemsBlock.getSlice(i, 0, itemsBlock.getSliceLength(i));
            String item = itemSlice.toStringUtf8();
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getLowerBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get the frequent items in a String ItemsSketch")
    @ScalarFunction("string_items_sketch_frequent_items")
    @SqlType("array(varchar)")
    public static Block itemsSketchFrequentItems(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BOOLEAN) boolean falsePositives)
    {
        StringItemsSketchProxy sketch = new StringItemsSketchProxy(slice);
        String[] frequentItems = sketch.getFrequentItems(falsePositives);
        BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, frequentItems.length);
        for (String frequentItem : frequentItems) {
            VarcharType.VARCHAR.writeSlice(blockBuilder, utf8Slice(frequentItem));
        }
        return blockBuilder.build();
    }
}
