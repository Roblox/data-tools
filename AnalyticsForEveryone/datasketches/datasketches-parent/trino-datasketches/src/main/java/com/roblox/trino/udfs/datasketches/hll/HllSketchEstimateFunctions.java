package com.roblox.trino.udfs.datasketches.hll;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public class HllSketchEstimateFunctions
{
    private HllSketchEstimateFunctions() {}

    @Description("Estimate the number of distinct values in an HLL sketch")
    @ScalarFunction("hll_count_distinct")
    @SqlType(StandardTypes.BIGINT)
    public static long hllSketchEstimate(
            @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        HllSketchProxy sketch = new HllSketchProxy(slice);
        return sketch.getEstimate();
    }

    @Description("Get an upper bound on the number of distinct values in an HLL sketch")
    @ScalarFunction("hll_count_distinct_ub")
    @SqlType(StandardTypes.BIGINT)
    public static long hllSketchUpperBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev)
    {
        HllSketchProxy sketch = new HllSketchProxy(slice);
        return sketch.getUpperBound(numStdDev);
    }

    @Description("Get a lower bound on the number of distinct values in an HLL sketch")
    @ScalarFunction("hll_count_distinct_lb")
    @SqlType(StandardTypes.BIGINT)
    public static long hllSketchLowerBound(
            @SqlType(StandardTypes.VARBINARY) Slice slice,
            @SqlType(StandardTypes.BIGINT) long numStdDev)
    {
        HllSketchProxy sketch = new HllSketchProxy(slice);
        return sketch.getLowerBound(numStdDev);
    }
}
