package io.github.cyrilsochor.kafky.core.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class FixedSizeMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;

    public FixedSizeMap(int size) {
        super(size + 2, 1F); //+2 to have place for the newly added element AND not fill up to cause resize
        this.maxSize = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }

}
