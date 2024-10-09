package com.roblox.trino.udfs.datasketches.hll;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class HllSketchStateFactory
        implements AccumulatorStateFactory<HllSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleHllSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedHllSketchState.class);

    @Override
    public HllSketchState createSingleState()
    {
        return new SingleHllSketchState();
    }

    @Override
    public HllSketchState createGroupedState()
    {
        return new GroupedHllSketchState();
    }

    public static class SingleHllSketchState
            implements HllSketchState
    {
        public HllSketchProxy sketch;

        @Override
        public HllSketchProxy getHllSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setHllSketchProxy(HllSketchProxy value)
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

    public static class GroupedHllSketchState
            implements GroupedAccumulatorState, HllSketchState
    {
        private final ObjectBigArray<HllSketchProxy> sketches = new ObjectBigArray<>();
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
        public HllSketchProxy getHllSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setHllSketchProxy(HllSketchProxy value)
        {
            if (getHllSketchProxy() != null) {
                size -= getHllSketchProxy().getEstimatedSize();
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
