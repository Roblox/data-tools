package com.roblox.trino.udfs.datasketches.doubleitems;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.StandardTypes;

public class DoubleItemsSketchEstimateFunctions
{
    private DoubleItemsSketchEstimateFunctions() {}

    @Description("Estimate the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchEstimate(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        return sketch.getEstimate(item);
    }

    @Description("Estimate the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchEstimateReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        double itemDouble = Float.intBitsToFloat((int) item);
        return sketch.getEstimate(itemDouble);
    }

    @Description("Estimate the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate")
    @SqlType("array(bigint)")
    public static Block itemsSketchEstimateArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            double item = DoubleType.DOUBLE.getDouble(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getEstimate(item));
        }
        return blockBuilder.build();
    }

    @Description("Estimate the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate")
    @SqlType("array(bigint)")
    public static Block itemsSketchEstimateRealArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long longItem = IntegerType.INTEGER.getLong(itemsBlock, i);
            double itemDouble = Float.intBitsToFloat((int) longItem);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getEstimate(itemDouble));
        }
        return blockBuilder.build();
    }

    @Description("Get an upper bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        return sketch.getUpperBound(item);
    }

    @Description("Get an upper bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchUpperBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        double itemDouble = Float.intBitsToFloat((int) item);
        return sketch.getUpperBound(itemDouble);
    }

    @Description("Get an upper bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_ub")
    @SqlType("array(bigint)")
    public static Block itemsSketchUpperBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            double item = DoubleType.DOUBLE.getDouble(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getUpperBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get an upper bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_ub")
    @SqlType("array(bigint)")
    public static Block itemsSketchUpperBoundRealArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long longItem = IntegerType.INTEGER.getLong(itemsBlock, i);
            double itemDouble = Float.intBitsToFloat((int) longItem);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getUpperBound(itemDouble));
        }
        return blockBuilder.build();
    }

    @Description("Get a lower bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        return sketch.getLowerBound(item);
    }

    @Description("Get a lower bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long itemsSketchLowerBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long item)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        double itemDouble = Float.intBitsToFloat((int) item);
        return sketch.getLowerBound(itemDouble);
    }

    @Description("Get a lower bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_lb")
    @SqlType("array(bigint)")
    public static Block itemsSketchLowerBoundArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            double item = DoubleType.DOUBLE.getDouble(itemsBlock, i);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getLowerBound(item));
        }
        return blockBuilder.build();
    }

    @Description("Get a lower bound on the frequency of an item in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_estimate_lb")
    @SqlType("array(bigint)")
    public static Block itemsSketchLowerBoundRealArray(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block itemsBlock)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        BlockBuilder blockBuilder = BigintType.BIGINT.createBlockBuilder(null, itemsBlock.getPositionCount());
        for (int i = 0; i < itemsBlock.getPositionCount(); i++) {
            long longItem = IntegerType.INTEGER.getLong(itemsBlock, i);
            double itemDouble = Float.intBitsToFloat((int) longItem);
            BigintType.BIGINT.writeLong(blockBuilder, sketch.getLowerBound(itemDouble));
        }
        return blockBuilder.build();
    }

    @Description("Get the frequent items in a double ItemsSketch")
    @ScalarFunction("double_items_sketch_frequent_items")
    @SqlType("array(double)")
    public static Block itemsSketchFrequentItems(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BOOLEAN) boolean falsePositives)
    {
        DoubleItemsSketchProxy sketch = new DoubleItemsSketchProxy(slice);
        double[] frequentItems = sketch.getFrequentItems(falsePositives);
        BlockBuilder blockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, frequentItems.length);
        for (double frequentItem : frequentItems) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, frequentItem);
        }
        return blockBuilder.build();
    }
}
