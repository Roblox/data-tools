package com.roblox.trino.udfs.datasketches.klldoubles;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.StandardTypes;

public class KllDoublesSketchEstimateFunctions
{
    private KllDoublesSketchEstimateFunctions() {}

    private static Block arrayToBlock(double[] arr)
    {
        BlockBuilder blockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, arr.length);
        for (double d : arr) {
            DoubleType.DOUBLE.writeDouble(blockBuilder, d);
        }
        return blockBuilder.build();
    }

    @Description("Estimate the value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantile(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateQuantile(quantile);
    }

    @Description("Estimate the value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantile(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the lower bound of a value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantileLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateQuantileLowerBound(quantile);
    }

    @Description("Estimate the lower bound of a value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileLowerBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantileLowerBound(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the upper bound of a value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateQuantileUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateQuantileUpperBound(quantile);
    }

    @Description("Estimate the upper bound of a value of a quantile in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_quantile_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateQuantileUpperBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block quantileBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] quantiles = new double[quantileBlock.getPositionCount()];
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = DoubleType.DOUBLE.getDouble(quantileBlock, i);
            quantiles[i] = sketch.getEstimateQuantileUpperBound(quantile);
        }
        return arrayToBlock(quantiles);
    }

    @Description("Estimate the rank of a value in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRank(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateRank(value);
    }

    @Description("Estimate the rank of a value in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double doubleValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRank(doubleValue);
    }

    @Description("Estimate the rank of a value in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRank(value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the rank of a value in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long longValue = IntegerType.INTEGER.getLong(valueBlock, i);
            double value = Float.intBitsToFloat((int) longValue);
            values[i] = sketch.getEstimateRank(value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the lower bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateRankLowerBound(value);
    }

    @Description("Estimate the lower bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_lb")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankLowerBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double doubleValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRankLowerBound(doubleValue);
    }

    @Description("Estimate the lower bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankLowerBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRankLowerBound(value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the lower bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_lb")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankLowerBoundRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long longValue = IntegerType.INTEGER.getLong(valueBlock, i);
            double value = Float.intBitsToFloat((int) longValue);
            values[i] = sketch.getEstimateRankLowerBound(value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the upper bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        return sketch.getEstimateRankUpperBound(value);
    }

    @Description("Estimate the upper bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_ub")
    @SqlType(StandardTypes.DOUBLE)
    public static double kllSketchEstimateRankUpperBoundReal(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.REAL) long value)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double doubleValue = Float.intBitsToFloat((int) value);
        return sketch.getEstimateRankUpperBound(doubleValue);
    }

    @Description("Estimate the upper bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankUpperBoundList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(double)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            double value = DoubleType.DOUBLE.getDouble(valueBlock, i);
            values[i] = sketch.getEstimateRankUpperBound(value);
        }
        return arrayToBlock(values);
    }

    @Description("Estimate the upper bound of a rank in a KLL doubles sketch")
    @ScalarFunction("kll_doubles_estimate_rank_ub")
    @SqlType("array(double)")
    public static Block kllSketchEstimateRankUpperBoundRealList(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType("array(real)") Block valueBlock)
    {
        KllDoublesSketchProxy sketch = new KllDoublesSketchProxy(slice);
        double[] values = new double[valueBlock.getPositionCount()];
        for (int i = 0; i < values.length; i++) {
            long longValue = IntegerType.INTEGER.getLong(valueBlock, i);
            double value = Float.intBitsToFloat((int) longValue);
            values[i] = sketch.getEstimateRankUpperBound(value);
        }
        return arrayToBlock(values);
    }
}
