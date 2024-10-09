package com.roblox.trino.udfs.datasketches.kllfloats;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class KllFloatsSketchStateFactory
        implements AccumulatorStateFactory<KllFloatsSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleKllFloatsSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedKllFloatsSketchState.class);

    @Override
    public KllFloatsSketchState createSingleState()
    {
        return new SingleKllFloatsSketchState();
    }

    @Override
    public KllFloatsSketchState createGroupedState()
    {
        return new GroupedKllFloatsSketchState();
    }

    public static class SingleKllFloatsSketchState
            implements KllFloatsSketchState
    {
        public KllFloatsSketchProxy sketch;

        @Override
        public KllFloatsSketchProxy getKllSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setKllSketchProxy(KllFloatsSketchProxy value)
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

    public static class GroupedKllFloatsSketchState
            implements GroupedAccumulatorState, KllFloatsSketchState
    {
        private final ObjectBigArray<KllFloatsSketchProxy> sketches = new ObjectBigArray<>();
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
        public KllFloatsSketchProxy getKllSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setKllSketchProxy(KllFloatsSketchProxy value)
        {
            if (getKllSketchProxy() != null) {
                size -= getKllSketchProxy().getEstimatedSize();
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
