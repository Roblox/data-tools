package com.roblox.trino.udfs.datasketches.klldoubles;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class KllDoublesSketchStateFactory
        implements AccumulatorStateFactory<KllDoublesSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleKllDoublesSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedKllDoublesSketchState.class);

    @Override
    public KllDoublesSketchState createSingleState()
    {
        return new SingleKllDoublesSketchState();
    }

    @Override
    public KllDoublesSketchState createGroupedState()
    {
        return new GroupedKllDoublesSketchState();
    }

    public static class SingleKllDoublesSketchState
            implements KllDoublesSketchState
    {
        public KllDoublesSketchProxy sketch;

        @Override
        public KllDoublesSketchProxy getKllSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setKllSketchProxy(KllDoublesSketchProxy value)
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

    public static class GroupedKllDoublesSketchState
            implements GroupedAccumulatorState, KllDoublesSketchState
    {
        private final ObjectBigArray<KllDoublesSketchProxy> sketches = new ObjectBigArray<>();
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
        public KllDoublesSketchProxy getKllSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setKllSketchProxy(KllDoublesSketchProxy value)
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
