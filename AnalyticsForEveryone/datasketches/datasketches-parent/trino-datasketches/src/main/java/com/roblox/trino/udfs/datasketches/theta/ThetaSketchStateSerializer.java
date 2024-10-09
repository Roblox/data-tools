package com.roblox.trino.udfs.datasketches.theta;

import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;

import java.nio.charset.StandardCharsets;

public class ThetaSketchStateSerializer
        implements AccumulatorStateSerializer<ThetaSketchState>
{
    private static final Logger log = Logger.get(ThetaSketchStateSerializer.class);

    @Override
    public Type getSerializedType()
    {
        return VarbinaryType.VARBINARY;
    }

    @Override
    public void serialize(ThetaSketchState state, BlockBuilder out)
    {
        if (state.getThetaSketchProxy() == null) {
            out.appendNull();
        }
        else {
            ThetaSketchProxy sketch = state.getThetaSketchProxy();
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(100);

            sliceOutput.appendInt(sketch.getK());
            sliceOutput.appendBytes(sketch.serialize());

            VarbinaryType.VARBINARY.writeSlice(out, sliceOutput.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, ThetaSketchState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VarbinaryType.VARBINARY.getSlice(block, index);
            BasicSliceInput input = slice.getInput();
            // The slice will contain an integer K followed by a Slice which contains the serialized sketch
            try {
                int k = input.readInt();
                Slice serialized = input.readSlice((int) input.length() - Integer.BYTES);
                state.setThetaSketchProxy(new ThetaSketchProxy(serialized, k));
            }
            catch (Exception e) {
                log.error("Error reading Theta sketch bytes, slice: {}", new String(slice.getBytes(), StandardCharsets.UTF_8));
            }
        }
    }
}
