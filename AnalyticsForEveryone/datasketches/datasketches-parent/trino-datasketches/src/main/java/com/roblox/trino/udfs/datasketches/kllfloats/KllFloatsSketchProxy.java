package com.roblox.trino.udfs.datasketches.kllfloats;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.WritableMemory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class KllFloatsSketchProxy
{
    public final KllFloatsSketch sketch;

    public KllFloatsSketchProxy()
    {
        this.sketch = KllFloatsSketch.newHeapInstance();
    }

    public KllFloatsSketchProxy(int k)
    {
        this.sketch = KllFloatsSketch.newHeapInstance(k);
    }

    public KllFloatsSketchProxy(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);
        this.sketch = KllFloatsSketch.wrap(memory);
    }

    public void put(float item)
    {
        this.sketch.update(item);
    }

    public void union(KllFloatsSketchProxy other)
    {
        this.sketch.merge(other.sketch);
    }

    public double getEstimateQuantile(double quantile)
    {
        return this.sketch.getQuantile(quantile);
    }

    public double getEstimateQuantileUpperBound(double quantile)
    {
        return this.sketch.getQuantileUpperBound(quantile);
    }

    public double getEstimateQuantileLowerBound(double quantile)
    {
        return this.sketch.getQuantileLowerBound(quantile);
    }

    public double getEstimateRank(float value)
    {
        return this.sketch.getRank(value);
    }

    public double getEstimateRankUpperBound(float value)
    {
        double rank = this.getEstimateRank(value);
        return this.sketch.getRankUpperBound(rank);
    }

    public double getEstimateRankLowerBound(float value)
    {
        double rank = this.getEstimateRank(value);
        return this.sketch.getRankLowerBound(rank);
    }

    public Slice serialize()
    {
        byte[] bytes = this.sketch.toByteArray();
        try (DynamicSliceOutput output = new DynamicSliceOutput(bytes.length)) {
            output.appendBytes(bytes);
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int getK()
    {
        return this.sketch.getK();
    }

    public long getEstimatedSize()
    {
        return this.sketch.getSerializedSizeBytes();
    }
}
