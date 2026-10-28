package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.Bytes.s;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

class MemtableTest {

    private final Memtable memtable = new Memtable(7);

    @Test
    void getReturnsTheNewestVersionVisibleAtTheSnapshot() {
        memtable.add(InternalEntry.put(b("k"), 1, b("v1"), NO_EXPIRY));
        memtable.add(InternalEntry.put(b("k"), 5, b("v5"), NO_EXPIRY));
        memtable.add(InternalEntry.tombstone(b("k"), 9));

        assertThat(memtable.get(b("k"), 0)).isNull();
        assertThat(s(memtable.get(b("k"), 1).value())).isEqualTo("v1");
        assertThat(s(memtable.get(b("k"), 8).value())).isEqualTo("v5");
        assertThat(memtable.get(b("k"), 9).isTombstone()).isTrue();
        assertThat(memtable.get(b("other"), Long.MAX_VALUE)).isNull();
    }

    @Test
    void keysAreOrderedAsUnsignedBytes() {
        memtable.add(InternalEntry.put(new byte[] {(byte) 0xFF}, 1, b("high"), NO_EXPIRY));
        memtable.add(InternalEntry.put(new byte[] {0x01}, 2, b("low"), NO_EXPIRY));

        List<String> values = values(memtable.iterator(null, null, Long.MAX_VALUE));

        assertThat(values).containsExactly("low", "high");
    }

    @Test
    void iteratorYieldsEveryVisibleVersionNewestFirstWithinBounds() {
        memtable.add(InternalEntry.put(b("a"), 1, b("a1"), NO_EXPIRY));
        memtable.add(InternalEntry.put(b("b"), 2, b("b2"), NO_EXPIRY));
        memtable.add(InternalEntry.put(b("b"), 3, b("b3"), NO_EXPIRY));
        memtable.add(InternalEntry.put(b("b"), 10, b("b10"), NO_EXPIRY));
        memtable.add(InternalEntry.put(b("c"), 4, b("c4"), NO_EXPIRY));

        assertThat(values(memtable.iterator(b("b"), b("c"), 5))).containsExactly("b3", "b2");
        assertThat(values(memtable.iterator(null, null, 5))).containsExactly("a1", "b3", "b2", "c4");
    }

    @Test
    void tracksSizeAndCount() {
        assertThat(memtable.isEmpty()).isTrue();

        memtable.add(InternalEntry.put(b("key"), 1, b("value"), NO_EXPIRY));

        assertThat(memtable.isEmpty()).isFalse();
        assertThat(memtable.entryCount()).isEqualTo(1);
        assertThat(memtable.approximateBytes()).isEqualTo(3 + 5 + Memtable.ENTRY_OVERHEAD_BYTES);
        assertThat(memtable.walNumber()).isEqualTo(7);
    }

    static List<String> values(Iterator<InternalEntry> entries) {
        List<String> values = new ArrayList<>();
        entries.forEachRemaining(entry -> values.add(entry.isTombstone() ? "<tombstone>" : s(entry.value())));
        return values;
    }
}
