package com.roblox.trino.udfs.datasketches.klldoubles;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

public class KllDoublesSketchStateSerializer
        implements AccumulatorStateSerializer<KllDoublesSketchState>
{
    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(KllDoublesSketchState state, BlockBuilder out)
    {
        if (state.getKllSketchProxy() == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, state.getKllSketchProxy().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, KllDoublesSketchState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
            state.setKllSketchProxy(new KllDoublesSketchProxy(slice));
        }
    }
}
