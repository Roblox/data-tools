package com.roblox.trino.udfs.datasketches.doubleitems;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.ItemsSketch.Row;
import org.apache.datasketches.memory.WritableMemory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class DoubleItemsSketchProxy
{
    public static final int DEFAULT_MAP_SIZE = 64;

    private final ItemsSketch<Double> sketch;

    public DoubleItemsSketchProxy()
    {
        this.sketch = new ItemsSketch<>(DEFAULT_MAP_SIZE);
    }

    public DoubleItemsSketchProxy(int mapSize)
    {
        this.sketch = new ItemsSketch<>(mapSize);
    }

    public DoubleItemsSketchProxy(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);
        this.sketch = ItemsSketch.getInstance(memory, new ArrayOfDoublesSerDe());
    }

    public void put(double item)
    {
        this.sketch.update(item);
    }

    public long getEstimate(double item)
    {
        return this.sketch.getEstimate(item);
    }

    public long getLowerBound(double item)
    {
        return this.sketch.getLowerBound(item);
    }

    public long getUpperBound(double item)
    {
        return this.sketch.getUpperBound(item);
    }

    public double[] getFrequentItems(boolean falsePositives)
    {
        Row<Double>[] results = this.sketch.getFrequentItems(falsePositives ? ErrorType.NO_FALSE_NEGATIVES : ErrorType.NO_FALSE_POSITIVES);
        double[] items = new double[results.length];
        for (int i = 0; i < results.length; i++) {
            items[i] = results[i].getItem();
        }
        return items;
    }

    public void merge(DoubleItemsSketchProxy other)
    {
        this.sketch.merge(other.getSketch());
    }

    public Slice serialize()
    {
        byte[] bytes = this.sketch.toByteArray(new ArrayOfDoublesSerDe());
        try (DynamicSliceOutput output = new DynamicSliceOutput(bytes.length)) {
            output.appendBytes(bytes);
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ItemsSketch<Double> getSketch()
    {
        return this.sketch;
    }

    public int getMaxMapSize()
    {
        return this.sketch.getMaximumMapCapacity() * 4 / 3; // 1 / 0.75 from ReversePurgeLongHashMap.LOAD_FACTOR
    }

    public long getEstimatedSize()
    {
        byte[] bytes = this.sketch.toByteArray(new ArrayOfDoublesSerDe());
        return bytes.length;
    }
}
