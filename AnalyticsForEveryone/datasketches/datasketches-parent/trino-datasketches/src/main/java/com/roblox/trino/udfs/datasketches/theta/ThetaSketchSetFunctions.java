package com.roblox.trino.udfs.datasketches.theta;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;

public class ThetaSketchSetFunctions
{
    private ThetaSketchSetFunctions() {}

    @Description("Union two theta sketches")
    @ScalarFunction("theta_union")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice thetaSketchUnion(
            @SqlType(StandardTypes.VARBINARY) Slice slice1,
            @SqlType(StandardTypes.VARBINARY) Slice slice2)
    {
        ThetaSketchProxy sketch1 = new ThetaSketchProxy(slice1, ThetaSketchProxy.DEFAULT_K);
        ThetaSketchProxy sketch2 = new ThetaSketchProxy(slice2, ThetaSketchProxy.DEFAULT_K);
        sketch1.union(sketch2);
        return sketch1.serialize();
    }

    @Description("Union two theta sketches with a provided k")
    @ScalarFunction("theta_union")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice thetaSketchUnionWithK(
            @SqlType(StandardTypes.VARBINARY) Slice slice1,
            @SqlType(StandardTypes.VARBINARY) Slice slice2,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy sketch1 = new ThetaSketchProxy(slice1, (int) k);
        ThetaSketchProxy sketch2 = new ThetaSketchProxy(slice2, (int) k);
        sketch1.union(sketch2);
        return sketch1.serialize();
    }

    @Description("Intersect two theta sketches")
    @ScalarFunction("theta_intersection")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice thetaSketchIntersection(
            @SqlType(StandardTypes.VARBINARY) Slice slice1,
            @SqlType(StandardTypes.VARBINARY) Slice slice2)
    {
        ThetaSketchProxy sketch1 = new ThetaSketchProxy(slice1, ThetaSketchProxy.DEFAULT_K);
        ThetaSketchProxy sketch2 = new ThetaSketchProxy(slice2, ThetaSketchProxy.DEFAULT_K);
        return intersectSketches(sketch1, sketch2);
    }

    @Description("Intersect two theta sketches with a provided k")
    @ScalarFunction("theta_intersection")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice thetaSketchIntersection(
            @SqlType(StandardTypes.VARBINARY) Slice slice1,
            @SqlType(StandardTypes.VARBINARY) Slice slice2,
            @SqlType(StandardTypes.BIGINT) long k)
    {
        ThetaSketchProxy sketch1 = new ThetaSketchProxy(slice1, (int) k);
        ThetaSketchProxy sketch2 = new ThetaSketchProxy(slice2, (int) k);
        return intersectSketches(sketch1, sketch2);
    }

    private static Slice intersectSketches(ThetaSketchProxy sketch1, ThetaSketchProxy sketch2)
    {
        Intersection intersection = SetOperation.builder().buildIntersection();
        intersection.intersect(sketch1.getSketch());
        intersection.intersect(sketch2.getSketch());
        byte[] bytes = intersection.getResult().compact().toByteArray();
        DynamicSliceOutput output = new DynamicSliceOutput(bytes.length);
        output.appendBytes(bytes);
        return output.slice();
    }
}
