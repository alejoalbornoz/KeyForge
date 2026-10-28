package io.keyforge.storage.lsm;

import static io.keyforge.storage.KeyValueStore.NO_EXPIRY;
import static io.keyforge.storage.lsm.Bytes.b;
import static io.keyforge.storage.lsm.MemtableTest.values;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

class MergingIteratorTest {

    @Test
    void keepsOnlyTheNewestVersionOfEachKeyAcrossSources() {
        Iterator<InternalEntry> newer = List.of(
                put("b", 20, "b-new"),
                InternalEntry.tombstone(b("c"), 21)).iterator();
        Iterator<InternalEntry> older = List.of(
                put("a", 1, "a-old"),
                put("b", 2, "b-old"),
                put("c", 3, "c-old"),
                put("d", 4, "d-old")).iterator();

        MergingIterator merged = new MergingIterator(List.of(newer, older));

        assertThat(values(merged)).containsExactly("a-old", "b-new", "<tombstone>", "d-old");
    }

    @Test
    void collapsesVersionsWithinOneSource() {
        Iterator<InternalEntry> memtable = List.of(put("k", 9, "v9"), put("k", 3, "v3"), put("m", 1, "m1")).iterator();

        assertThat(values(new MergingIterator(List.of(memtable)))).containsExactly("v9", "m1");
    }

    @Test
    void theEarlierSourceWinsATie() {
        Iterator<InternalEntry> first = List.of(put("k", 5, "first")).iterator();
        Iterator<InternalEntry> second = List.of(put("k", 5, "second")).iterator();

        assertThat(values(new MergingIterator(List.of(first, second)))).containsExactly("first");
    }

    @Test
    void handlesNoSources() {
        assertThat(new MergingIterator(List.of(Collections.emptyIterator())).hasNext()).isFalse();
        assertThat(new MergingIterator(List.of()).hasNext()).isFalse();
    }

    private static InternalEntry put(String key, long seq, String value) {
        return InternalEntry.put(b(key), seq, b(value), NO_EXPIRY);
    }
}
