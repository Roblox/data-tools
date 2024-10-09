package com.roblox.trino.udfs.datasketches.theta;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public class ThetaSketchEstimateFunctions
{
    private ThetaSketchEstimateFunctions() {}

    @Description("Estimate the number of distinct values in a theta sketch")
    @ScalarFunction("theta_count_distinct")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchEstimate(
            @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, ThetaSketchProxy.DEFAULT_K);
        return sketch.getEstimate();
    }

    @Description("Estimate the number of distinct values in a theta sketch, with a provided k")
    @ScalarFunction("theta_count_distinct")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchEstimateWithK(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, (int) k);
        return sketch.getEstimate();
    }

    @Description("Get an upper bound on the number of distinct values in a theta sketch")
    @ScalarFunction("theta_count_distinct_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, ThetaSketchProxy.DEFAULT_K);
        return sketch.getUpperBound(numStdDev);
    }

    @Description("Get an upper bound on the number of distinct values in a theta sketch, with a provided k")
    @ScalarFunction("theta_count_distinct_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchUpperBoundWithK(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, (int) k);
        return sketch.getUpperBound(numStdDev);
    }

    @Description("Get a lower bound on the number of distinct values in a theta sketch")
    @ScalarFunction("theta_count_distinct_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, ThetaSketchProxy.DEFAULT_K);
        return sketch.getLowerBound(numStdDev);
    }

    @Description("Get a lower bound on the number of distinct values in a theta sketch, with a provided k")
    @ScalarFunction("theta_count_distinct_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long thetaSketchLowerBoundWithK(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy sketch = new ThetaSketchProxy(slice, (int) k);
        return sketch.getLowerBound(numStdDev);
    }
}
