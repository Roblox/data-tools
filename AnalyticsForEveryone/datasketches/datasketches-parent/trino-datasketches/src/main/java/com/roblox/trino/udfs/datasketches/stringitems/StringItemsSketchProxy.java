package com.roblox.trino.udfs.datasketches.stringitems;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.ItemsSketch.Row;
import org.apache.datasketches.memory.WritableMemory;

import java.io.IOException;
import java.io.UncheckedIOException;

public class StringItemsSketchProxy
{
    public static final int DEFAULT_MAP_SIZE = 64;

    private final ItemsSketch<String> sketch;

    public StringItemsSketchProxy()
    {
        this.sketch = new ItemsSketch<>(DEFAULT_MAP_SIZE);
    }

    public StringItemsSketchProxy(int mapSize)
    {
        this.sketch = new ItemsSketch<>(mapSize);
    }

    public StringItemsSketchProxy(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        int length = (int) input.length();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        WritableMemory memory = WritableMemory.writableWrap(bytes);
        this.sketch = ItemsSketch.getInstance(memory, new ArrayOfStringsSerDe());
    }

    public void put(String item)
    {
        this.sketch.update(item);
    }

    public long getEstimate(String item)
    {
        return this.sketch.getEstimate(item);
    }

    public long getLowerBound(String item)
    {
        return this.sketch.getLowerBound(item);
    }

    public long getUpperBound(String item)
    {
        return this.sketch.getUpperBound(item);
    }

    public String[] getFrequentItems(boolean falsePositives)
    {
        Row<String>[] results = this.sketch.getFrequentItems(falsePositives ? ErrorType.NO_FALSE_NEGATIVES : ErrorType.NO_FALSE_POSITIVES);
        String[] items = new String[results.length];
        for (int i = 0; i < results.length; i++) {
            items[i] = results[i].getItem();
        }
        return items;
    }

    public void merge(StringItemsSketchProxy other)
    {
        this.sketch.merge(other.getSketch());
    }

    public Slice serialize()
    {
        byte[] bytes = this.sketch.toByteArray(new ArrayOfStringsSerDe());
        try (DynamicSliceOutput output = new DynamicSliceOutput(bytes.length)) {
            output.appendBytes(bytes);
            return output.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ItemsSketch<String> getSketch()
    {
        return this.sketch;
    }

    public int getMaxMapSize()
    {
        return this.sketch.getMaximumMapCapacity() * 4 / 3; // 1 / 0.75 from ReversePurgeLongHashMap.LOAD_FACTOR
    }

    public long getEstimatedSize()
    {
        byte[] bytes = this.sketch.toByteArray(new ArrayOfStringsSerDe());
        return bytes.length;
    }
}
