package io.keyforge.storage.lsm;

/**
 * Chooses which SSTables to merge: size-tiered compaction (STCS) restricted to tables that are adjacent in age.
 *
 * <p>Tables are listed newest first. A candidate is a run of adjacent tables whose sizes are all within
 * {@code [bucketLow, bucketHigh]} times the run's average, or that are all below {@code smallTableBytes} (tiny tables are
 * treated as one tier, as Cassandra does). A run qualifies with at least {@code minThreshold} tables and is cut at
 * {@code maxThreshold}. Among qualifying runs the one with the smallest average size wins: merging small tables is
 * cheapest and removes the most tables per byte written.
 *
 * <p>Restricting runs to adjacent tables keeps every table's sequence-number range disjoint from the others, so a point
 * lookup can stop at the first table that has the key, and a merge that includes the oldest table can drop tombstones.
 * Since each merge replaces a run in place, sizes grow with age and adjacent tables tend to be similar in size anyway.
 *
 * @param minThreshold    fewest tables worth merging
 * @param maxThreshold    most tables merged at once
 * @param smallTableBytes tables below this size all count as one tier
 * @param bucketLow       smallest size, relative to the run's average, that still joins the run
 * @param bucketHigh      largest size, relative to the run's average, that still joins the run
 */
record SizeTieredCompactionPicker(int minThreshold, int maxThreshold, long smallTableBytes, double bucketLow,
                                  double bucketHigh) {

    /** A run of tables, {@code [from, to)} in the newest-first list. */
    record Window(int from, int to) {

        int size() {
            return to - from;
        }
    }

    SizeTieredCompactionPicker {
        if (minThreshold < 2 || maxThreshold < minThreshold) {
            throw new IllegalArgumentException("Need 2 <= minThreshold <= maxThreshold, got " + minThreshold + " and "
                    + maxThreshold);
        }
        if (smallTableBytes < 0 || bucketLow <= 0 || bucketLow > 1 || bucketHigh < 1) {
            throw new IllegalArgumentException("Invalid bucket bounds");
        }
    }

    static SizeTieredCompactionPicker withDefaults(int minThreshold, long smallTableBytes) {
        return new SizeTieredCompactionPicker(minThreshold, Math.max(minThreshold, 32), smallTableBytes, 0.5, 1.5);
    }

    /** @param sizes table sizes in bytes, newest first; returns the run to merge, or {@code null} if none qualifies */
    Window pick(long[] sizes) {
        Window best = null;
        double bestAverage = Double.MAX_VALUE;
        for (int from = 0; from < sizes.length; from++) {
            long total = sizes[from];
            int to = from + 1;
            while (to < sizes.length && to - from < maxThreshold) {
                double average = (double) total / (to - from);
                long size = sizes[to];
                boolean bothSmall = size < smallTableBytes && average < smallTableBytes;
                boolean similar = size >= average * bucketLow && size <= average * bucketHigh;
                if (!bothSmall && !similar) {
                    break;
                }
                total += size;
                to++;
            }
            if (to - from >= minThreshold) {
                double average = (double) total / (to - from);
                if (average < bestAverage) {
                    best = new Window(from, to);
                    bestAverage = average;
                }
            }
        }
        return best;
    }
}
