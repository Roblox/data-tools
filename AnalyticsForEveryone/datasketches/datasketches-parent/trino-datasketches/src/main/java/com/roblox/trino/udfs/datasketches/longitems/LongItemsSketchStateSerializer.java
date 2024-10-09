package com.roblox.trino.udfs.datasketches.longitems;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

public class LongItemsSketchStateSerializer
        implements AccumulatorStateSerializer<LongItemsSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(LongItemsSketchState state, BlockBuilder out)
    {
        if (state.getItemsSketchProxy() == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, state.getItemsSketchProxy().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, LongItemsSketchState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
            state.setItemsSketchProxy(new LongItemsSketchProxy(slice));
        }
    }
}
