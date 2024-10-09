package com.roblox.trino.udfs.datasketches.hll;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.WritableMemory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class HllSketchProxy
{
    public static final int DEFAULT_LG_K = HllSketch.DEFAULT_LG_K;
    public static final TgtHllType DEFAULT_HLL_TYPE = TgtHllType.HLL_6;

    private final HllSketch sketch;

    public HllSketchProxy()
    {
        this.sketch = new HllSketch(DEFAULT_LG_K, DEFAULT_HLL_TYPE);
    }

    public HllSketchProxy(int lgK)
    {
        this.sketch = new HllSketch(lgK, DEFAULT_HLL_TYPE);
    }

    public HllSketchProxy(HllSketch sketch)
    {
        this.sketch = sketch;
    }

    public HllSketchProxy(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);
        this.sketch = HllSketch.wrap(memory);
    }

    public void put(String item)
    {
        this.sketch.update(item);
    }

    public void put(double item)
    {
        this.sketch.update(item);
    }

    public void put(long item)
    {
        this.sketch.update(item);
    }

    public long getEstimate()
    {
        return (long) this.sketch.getEstimate();
    }

    public long getUpperBound(long numStdDev)
    {
        return (long) this.sketch.getUpperBound((int) numStdDev);
    }

    public long getLowerBound(long numStdDev)
    {
        return (long) this.sketch.getLowerBound((int) numStdDev);
    }

    public Slice serialize()
    {
        byte[] bytes = this.sketch.toCompactByteArray();
        try (DynamicSliceOutput output = new DynamicSliceOutput(bytes.length)) {
            output.appendBytes(bytes);
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public HllSketch getSketch()
    {
        return this.sketch;
    }

    public int getLgK()
    {
        return this.sketch.getLgConfigK();
    }

    public long getEstimatedSize()
    {
        return this.sketch.getCompactSerializationBytes();
    }
}
