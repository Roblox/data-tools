package com.roblox.trino.udfs.datasketches.hll;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

public class HllSketchStateSerializer
        implements AccumulatorStateSerializer<HllSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(HllSketchState state, BlockBuilder out)
    {
        if (state.getHllSketchProxy() == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, state.getHllSketchProxy().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HllSketchState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
            state.setHllSketchProxy(new HllSketchProxy(slice));
        }
    }
}
