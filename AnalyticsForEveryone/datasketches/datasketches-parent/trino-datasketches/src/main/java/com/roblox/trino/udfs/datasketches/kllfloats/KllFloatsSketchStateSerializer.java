package com.roblox.trino.udfs.datasketches.kllfloats;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

public class KllFloatsSketchStateSerializer
        implements AccumulatorStateSerializer<KllFloatsSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(KllFloatsSketchState state, BlockBuilder out)
    {
        if (state.getKllSketchProxy() == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, state.getKllSketchProxy().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, KllFloatsSketchState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
            state.setKllSketchProxy(new KllFloatsSketchProxy(slice));
        }
    }
}
