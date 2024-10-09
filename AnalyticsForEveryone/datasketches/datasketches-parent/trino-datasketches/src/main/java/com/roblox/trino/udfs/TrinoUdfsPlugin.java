package com.roblox.trino.udfs;

import com.google.common.collect.ImmutableSet;
import com.roblox.trino.udfs.datasketches.doubleitems.DoubleItemsSketchAggFunction;
import com.roblox.trino.udfs.datasketches.doubleitems.DoubleItemsSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.hll.HllSketchAggFunction;
import com.roblox.trino.udfs.datasketches.hll.HllSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.klldoubles.KllDoublesSketchAggFunction;
import com.roblox.trino.udfs.datasketches.klldoubles.KllDoublesSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.kllfloats.KllFloatsSketchAggFunction;
import com.roblox.trino.udfs.datasketches.kllfloats.KllFloatsSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.longitems.LongItemsSketchAggFunction;
import com.roblox.trino.udfs.datasketches.longitems.LongItemsSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.stringitems.StringItemsSketchAggFunction;
import com.roblox.trino.udfs.datasketches.stringitems.StringItemsSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.theta.ThetaSketchAggFunction;
import com.roblox.trino.udfs.datasketches.theta.ThetaSketchEstimateFunctions;
import com.roblox.trino.udfs.datasketches.theta.ThetaSketchSetFunctions;
import io.trino.spi.Plugin;

import java.util.Set;

public class TrinoUdfsPlugin
        implements Plugin
{

    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(HllSketchEstimateFunctions.class)
                .add(HllSketchAggFunction.class)
                .add(ThetaSketchEstimateFunctions.class)
                .add(ThetaSketchAggFunction.class)
                .add(ThetaSketchSetFunctions.class)
                .add(KllDoublesSketchAggFunction.class)
                .add(KllDoublesSketchEstimateFunctions.class)
                .add(KllFloatsSketchAggFunction.class)
                .add(KllFloatsSketchEstimateFunctions.class)
                .add(StringItemsSketchAggFunction.class)
                .add(StringItemsSketchEstimateFunctions.class)
                .add(DoubleItemsSketchEstimateFunctions.class)
                .add(DoubleItemsSketchAggFunction.class)
                .add(LongItemsSketchEstimateFunctions.class)
                .add(LongItemsSketchAggFunction.class)
                .build();
    }
}
