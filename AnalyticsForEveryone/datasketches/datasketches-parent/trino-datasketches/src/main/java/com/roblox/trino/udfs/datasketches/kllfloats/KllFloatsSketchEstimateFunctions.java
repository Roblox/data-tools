package com.roblox.trino.udfs.datasketches.kllfloats;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.StandardTypes;

public class KllFloatsSketchEstimateFunctions
{
    private KllFloatsSketchEstimateFunctions() {}

    private static Block arrayToBlock(double[] arr)
    {
        BlockBuilder blockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, arr.length);
        for (double d : arr) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, d);
        }
        return blockBuilder.build();
    }

    @Description("Estimate the value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantile(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateQuantile(quantile);
    }

    @Description("Estimate the value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantile(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the lower bound of a value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantileLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateQuantileLowerBound(quantile);
    }

    @Description("Estimate the lower bound of a value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileLowerBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantileLowerBound(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the upper bound of a value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantileUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateQuantileUpperBound(quantile);
    }

    @Description("Estimate the upper bound of a value of a quantile in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_quantile_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileUpperBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantileUpperBound(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the rank of a value in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRank(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateRank((float) value);
    }

    @Description("Estimate the rank of a value in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        float floatValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRank(floatValue);
    }

    @Description("Estimate the rank of a value in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRank((float) value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the rank of a value in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long value = IntegerType.INTEGER.getLong(valueBlock, i);
            float floatValue = Float.intBitsToFloat((int) value);
            values[i] = sketch.getEstimateRank(floatValue);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the lower bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateRankLowerBound((float) value);
    }

    @Description("Estimate the lower bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankLowerBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        float floatValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRankLowerBound(floatValue);
    }

    @Description("Estimate the lower bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankLowerBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRankLowerBound((float) value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the lower bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankLowerBoundRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long value = IntegerType.INTEGER.getLong(valueBlock, i);
            float floatValue = Float.intBitsToFloat((int) value);
            values[i] = sketch.getEstimateRankLowerBound(floatValue);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the upper bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        return sketch.getEstimateRankUpperBound((float) value);
    }

    @Description("Estimate the upper bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankUpperBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        float floatValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRankUpperBound(floatValue);
    }

    @Description("Estimate the upper bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankUpperBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRankUpperBound((float) value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the upper bound of a rank in a KLL floats sketch")
    @ScalarFunction("kll_floats_estimate_rank_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankUpperBoundRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllFloatsSketchProxy sketch = new KllFloatsSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long value = IntegerType.INTEGER.getLong(valueBlock, i);
            float floatValue = Float.intBitsToFloat((int) value);
            values[i] = sketch.getEstimateRankUpperBound(floatValue);
        }
        return arrayToBlock(values);
    }
}
