package com.roblox.trino.udfs.datasketches.longitems;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class LongItemsSketchStateFactory
        implements AccumulatorStateFactory<LongItemsSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleLongItemsSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedLongItemsSketchState.class);

    @Override
    public LongItemsSketchState createSingleState()
    {
        return new SingleLongItemsSketchState();
    }

    @Override
    public LongItemsSketchState createGroupedState()
    {
        return new GroupedLongItemsSketchState();
    }

    public static class SingleLongItemsSketchState
            implements LongItemsSketchState
    {
        public LongItemsSketchProxy sketch;

        @Override
        public LongItemsSketchProxy getItemsSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setItemsSketchProxy(LongItemsSketchProxy value)
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

    public static class GroupedLongItemsSketchState
            implements GroupedAccumulatorState, LongItemsSketchState
    {
        private final ObjectBigArray<LongItemsSketchProxy> sketches = new ObjectBigArray<>();
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
        public LongItemsSketchProxy getItemsSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setItemsSketchProxy(LongItemsSketchProxy value)
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
