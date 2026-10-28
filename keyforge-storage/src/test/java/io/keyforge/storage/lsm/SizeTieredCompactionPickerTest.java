package io.keyforge.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SizeTieredCompactionPickerTest {

    private static final long MB = 1 << 20;

    private final SizeTieredCompactionPicker picker = new SizeTieredCompactionPicker(4, 32, 0, 0.5, 1.5);

    @Test
    void picksNothingBelowTheThreshold() {
        assertThat(picker.pick(new long[] {10 * MB, 10 * MB, 10 * MB})).isNull();
        assertThat(picker.pick(new long[0])).isNull();
    }

    @Test
    void picksARunOfSimilarSizes() {
        assertThat(picker.pick(new long[] {10 * MB, 11 * MB, 9 * MB, 10 * MB}))
                .isEqualTo(new SizeTieredCompactionPicker.Window(0, 4));
    }

    @Test
    void aMuchLargerTableEndsTheRun() {
        long[] sizes = {10 * MB, 10 * MB, 10 * MB, 10 * MB, 100 * MB, 100 * MB};

        assertThat(picker.pick(sizes)).isEqualTo(new SizeTieredCompactionPicker.Window(0, 4));
    }

    @Test
    void prefersTheTierWithTheSmallestTables() {
        long[] sizes = {1 * MB, 1 * MB, 1 * MB, 1 * MB, 40 * MB, 40 * MB, 40 * MB, 40 * MB};

        assertThat(picker.pick(sizes)).isEqualTo(new SizeTieredCompactionPicker.Window(0, 4));
    }

    @Test
    void findsAnOlderTierWhenTheNewestIsTooShort() {
        long[] sizes = {1 * MB, 1 * MB, 40 * MB, 40 * MB, 40 * MB, 40 * MB, 900 * MB};

        assertThat(picker.pick(sizes)).isEqualTo(new SizeTieredCompactionPicker.Window(2, 6));
    }

    @Test
    void onlyMergesAdjacentTables() {
        // Four 10 MB tables, but a 100 MB table separates them: no run of four adjacent similar tables exists.
        long[] sizes = {10 * MB, 10 * MB, 100 * MB, 10 * MB, 10 * MB};

        assertThat(picker.pick(sizes)).isNull();
    }

    @Test
    void smallTablesFormOneTierWhateverTheirSizes() {
        SizeTieredCompactionPicker withSmallTier = new SizeTieredCompactionPicker(4, 32, 4 * MB, 0.5, 1.5);
        long[] sizes = {100_000, 2 * MB, 10_000, 3 * MB, 50 * MB};

        assertThat(withSmallTier.pick(sizes)).isEqualTo(new SizeTieredCompactionPicker.Window(0, 4));
    }

    @Test
    void cutsRunsAtTheMaximum() {
        SizeTieredCompactionPicker capped = new SizeTieredCompactionPicker(2, 3, 0, 0.5, 1.5);

        assertThat(capped.pick(new long[] {MB, MB, MB, MB, MB})).isEqualTo(new SizeTieredCompactionPicker.Window(0, 3));
    }

    @Test
    void rejectsInvalidThresholds() {
        assertThatThrownBy(() -> new SizeTieredCompactionPicker(1, 32, 0, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SizeTieredCompactionPicker(4, 3, 0, 0.5, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
