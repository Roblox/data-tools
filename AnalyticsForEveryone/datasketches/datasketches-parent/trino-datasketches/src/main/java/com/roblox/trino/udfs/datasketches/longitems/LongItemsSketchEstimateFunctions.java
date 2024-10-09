package com.roblox.trino.udfs.datasketches.longitems;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

public class LongItemsSketchEstimateFunctions
{
    private LongItemsSketchEstimateFunctions() {}

    @Description("Estimate the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchEstimate(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long item)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        return sketch.getEstimate(item);
    }

    @Description("Estimate the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate")
    @SqlType("array(bigint)")
    public static Block itemsSketchEstimateArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(bigint)") Block itemsBlock)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long item = BigintType.BIGINT.getLong(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getEstimate(item));
        }
        return blockBuilder.build();
    }

    @Description("Get an upper bound on the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long item)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        return sketch.getUpperBound(item);
    }

    @Description("Get an upper bound on the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate_ub")
    @SqlType("array(bigint)")
    public static Block itemsSketchUpperBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(bigint)") Block itemsBlock)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long item = BigintType.BIGINT.getLong(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getUpperBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get a lower bound on the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long item)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        return sketch.getLowerBound(item);
    }

    @Description("Get a lower bound on the frequency of an item in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_estimate_lb")
    @SqlType("array(bigint)")
    public static Block itemsSketchLowerBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(bigint)") Block itemsBlock)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long item = BigintType.BIGINT.getLong(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getLowerBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get the frequent items in a long ItemsSketch")
    @ScalarFunction("long_items_sketch_frequent_items")
    @SqlType("array(bigint)")
    public static Block itemsSketchFrequentItems(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BOOLEAN) boolean falsePositives)
    {
        LongItemsSketchProxy sketch = new LongItemsSketchProxy(slice);
        long[] frequentItems = sketch.getFrequentItems(falsePositives);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, frequentItems.length);
        for (long frequentItem : frequentItems) {
            BigintType.BIGINT.writeLong(blockBuilder, frequentItem);
        }
        return blockBuilder.build();
    }
}
