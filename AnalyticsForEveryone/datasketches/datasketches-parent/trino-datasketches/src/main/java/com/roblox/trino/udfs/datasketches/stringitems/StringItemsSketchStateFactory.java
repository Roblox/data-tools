package com.roblox.trino.udfs.datasketches.stringitems;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class StringItemsSketchStateFactory
        implements AccumulatorStateFactory<StringItemsSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleStringItemsSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedStringItemsSketchState.class);

    @Override
    public StringItemsSketchState createSingleState()
    {
        return new SingleStringItemsSketchState();
    }

    @Override
    public StringItemsSketchState createGroupedState()
    {
        return new GroupedStringItemsSketchState();
    }

    public static class SingleStringItemsSketchState
            implements StringItemsSketchState
    {
        public StringItemsSketchProxy sketch;

        @Override
        public StringItemsSketchProxy getItemsSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setItemsSketchProxy(StringItemsSketchProxy value)
        {
            this.sketch = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (sketch == null) {
                return SIZE_OF_SINGLE;
            }
            return sketch.getEstimatedSize() + SIZE_OF_SINGLE;
        }
    }

    public static class GroupedStringItemsSketchState
            implements GroupedAccumulatorState, StringItemsSketchState
    {
        private final ObjectBigArray<StringItemsSketchProxy> sketches = new ObjectBigArray<>();
        private long groupId;
        private long size;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            sketches.ensureCapacity(size);
        }

        @Override
        public StringItemsSketchProxy getItemsSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setItemsSketchProxy(StringItemsSketchProxy value)
        {
            if (getItemsSketchProxy() != null) {
                size -= getItemsSketchProxy().getEstimatedSize();
            }
            size += value.getEstimatedSize();
            sketches.set(groupId, value);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + sketches.sizeOf() + SIZE_OF_GROUPED;
        }
    }
}
