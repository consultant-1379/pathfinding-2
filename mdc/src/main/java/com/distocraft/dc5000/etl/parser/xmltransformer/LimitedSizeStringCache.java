package com.distocraft.dc5000.etl.parser.xmltransformer;


public class LimitedSizeStringCache {

  private static final int SIZE = 12;
  
  private final String[] keys = new String[SIZE];

  private final Object[] vals = new Object[SIZE];

  private int latest = 0;

  public static final long[] stats = { 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L };
  
  LimitedSizeStringCache() {
  }

  public synchronized Object get(final String key) {
    if (key == null) {
      return null;
    }

    for (int i = 0; i < SIZE; i++) {
      if (key.equals(keys[(latest + i) % SIZE])) {
        final long previous = stats[i];
        stats[i] = (previous + 1L);
        return vals[(latest+i) % SIZE];
      }
    }

    final long previous = stats[SIZE];
    stats[SIZE] = (previous + 1L);
    return null;
  }

  public synchronized void put(final String key, final Object val) {
    if (get(key) == null) {
      latest = (latest + SIZE - 1) % SIZE;
      keys[latest] = key;
      vals[latest] = val;
    }
  }

}
