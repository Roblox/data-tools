package com.roblox.trino.udfs.datasketches.longitems;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.frequencies.LongsSketch.Row;
import org.apache.datasketches.memory.WritableMemory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class LongItemsSketchProxy
{
    public static final int DEFAULT_MAP_SIZE = 64;

    private final LongsSketch sketch;

    public LongItemsSketchProxy()
    {
        this.sketch = new LongsSketch(DEFAULT_MAP_SIZE);
    }

    public LongItemsSketchProxy(int mapSize)
    {
        this.sketch = new LongsSketch(mapSize);
    }

    public LongItemsSketchProxy(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);
        this.sketch = LongsSketch.getInstance(memory);
    }

    public void put(long item)
    {
        this.sketch.update(item);
    }

    public long getEstimate(long item)
    {
        return this.sketch.getEstimate(item);
    }

    public long getLowerBound(long item)
    {
        return this.sketch.getLowerBound(item);
    }

    public long getUpperBound(long item)
    {
        return this.sketch.getUpperBound(item);
    }

    public long[] getFrequentItems(boolean falsePositives)
    {
        Row[] results = this.sketch.getFrequentItems(falsePositives ? ErrorType.NO_FALSE_NEGATIVES : ErrorType.NO_FALSE_POSITIVES);
        long[] items = new long[results.length];
        for (int i = 0; i < results.length; i++) {
            items[i] = results[i].getItem();
        }
        return items;
    }

    public void merge(LongItemsSketchProxy other)
    {
        this.sketch.merge(other.getSketch());
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

    public LongsSketch getSketch()
    {
        return this.sketch;
    }

    public int getMaxMapSize()
    {
        return this.sketch.getMaximumMapCapacity() * 4 / 3; // 1 / 0.75 from ReversePurgeLongHashMap.LOAD_FACTOR
    }

    public long getEstimatedSize()
    {
        byte[] bytes = this.sketch.toByteArray();
        return bytes.length;
    }
}
