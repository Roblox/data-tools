package com.roblox.trino.udfs.datasketches.theta;

import io.trino.array.ObjectBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import static io.airlift.slice.SizeOf.instanceSize;

public class ThetaSketchStateFactory
        implements AccumulatorStateFactory<ThetaSketchState>
{
    private static final int SIZE_OF_SINGLE = instanceSize(SingleThetaSketchState.class);
    private static final int SIZE_OF_GROUPED = instanceSize(GroupedThetaSketchState.class);

    @Override
    public ThetaSketchState createSingleState()
    {
        return new SingleThetaSketchState();
    }

    @Override
    public ThetaSketchState createGroupedState()
    {
        return new GroupedThetaSketchState();
    }

    public static class SingleThetaSketchState
            implements ThetaSketchState
    {
        public ThetaSketchProxy sketch;

        @Override
        public ThetaSketchProxy getThetaSketchProxy()
        {
            return sketch;
        }

        @Override
        public void setThetaSketchProxy(ThetaSketchProxy value)
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

    public static class GroupedThetaSketchState
            implements GroupedAccumulatorState, ThetaSketchState
    {
        private final ObjectBigArray<ThetaSketchProxy> sketches = new ObjectBigArray<>();
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
        public ThetaSketchProxy getThetaSketchProxy()
        {
            return sketches.get(groupId);
        }

        @Override
        public void setThetaSketchProxy(ThetaSketchProxy value)
        {
            if (getThetaSketchProxy() != null) {
                size -= getThetaSketchProxy().getEstimatedSize();
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
