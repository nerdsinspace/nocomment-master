package nocomment.master.tracking;

import nocomment.master.util.ChunkPos;

import java.util.List;

public abstract class AbstractFilterMode {

    public abstract List<ChunkPos> updateStep(List<ChunkPos> hits, List<ChunkPos> misses);

    public abstract boolean includesBroadly(ChunkPos pos);

    public abstract void decommission();

    public FilterModeEnum getEnum() {
        return FilterModeEnum.CLASS_MAP.get(getClass());
    }
}
