package com.roblox.trino.udfs.datasketches.theta;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

import java.io.IOException;
import java.io.UncheckedIOException;

public class ThetaSketchProxy
{
    private final UpdateSketch sketch;
    private final Union union;
    private final int k;

    public static final int DEFAULT_K = 1 << UpdateSketch.builder().getLgNominalEntries();

    public ThetaSketchProxy()
    {
        this.sketch = UpdateSketch.builder().build();
        this.union = SetOperation.builder().buildUnion();
        this.k = DEFAULT_K;
    }

    public ThetaSketchProxy(int k)
    {
        this.sketch = UpdateSketch.builder().setNominalEntries(k).build();
        this.union = SetOperation.builder().setNominalEntries(k).buildUnion();
        this.k = k;
    }

    public ThetaSketchProxy(Slice slice, int k)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);

        this.sketch = UpdateSketch.builder().setNominalEntries(k).build();
        this.union = SetOperation.builder().setNominalEntries(k).buildUnion();
        this.union.union(Sketch.wrap(memory));
        this.k = k;
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

    public void union(ThetaSketchProxy other)
    {
        this.union.union(other.getSketch());
    }

    public long getEstimate()
    {
        return (long) this.getSketch().getEstimate();
    }

    public long getUpperBound(long numStdDev)
    {
        return (long) this.getSketch().getUpperBound((int) numStdDev);
    }

    public long getLowerBound(long numStdDev)
    {
        return (long) this.getSketch().getLowerBound((int) numStdDev);
    }

    public Slice serialize()
    {
        byte[] bytes = this.getSketch().compact().toByteArray();
        try (DynamicSliceOutput output = new DynamicSliceOutput(bytes.length)) {
            output.appendBytes(bytes);
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Sketch getSketch()
    {
        this.union.union(this.sketch);
        this.sketch.reset();
        return this.union.getResult();
    }

    public int getK()
    {
        return this.k;
    }

    public long getEstimatedSize()
    {
        return this.sketch.getCompactBytes();
    }
}
