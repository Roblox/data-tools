package com.roblox.trino.udfs.datasketches.doubleitems;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class DoubleItemsSketchStateFactory
        implements AccumulatorStateFactory<DoubleItemsSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleDoubleItemsSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedDoubleItemsSketchState.class);

    @Override
    public DoubleItemsSketchState createSingleState()
    {
        return new SingleDoubleItemsSketchState();
    }

    @Override
    public DoubleItemsSketchState createGroupedState()
    {
        return new GroupedDoubleItemsSketchState();
    }

    public static class SingleDoubleItemsSketchState
            implements DoubleItemsSketchState
    {
        public DoubleItemsSketchProxy sketch;

        @Override
        public DoubleItemsSketchProxy getItemsSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setItemsSketchProxy(DoubleItemsSketchProxy value)
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

    public static class GroupedDoubleItemsSketchState
            implements GroupedAccumulatorState, DoubleItemsSketchState
    {
        private final ObjectBigArray<DoubleItemsSketchProxy> sketches = new ObjectBigArray<>();
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
        public DoubleItemsSketchProxy getItemsSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setItemsSketchProxy(DoubleItemsSketchProxy value)
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
