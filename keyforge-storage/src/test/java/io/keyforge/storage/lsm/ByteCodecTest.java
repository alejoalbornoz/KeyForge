package io.keyforge.storage.lsm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class ByteCodecTest {

    @Property
    void varIntsRoundTrip(@ForAll int value) {
        ByteWriter out = new ByteWriter(1);
        out.writeVarInt(value);

        ByteReader in = new ByteReader(out.toByteArray(), "test");
        assertThat(in.readVarInt()).isEqualTo(value);
        assertThat(in.hasRemaining()).isFalse();
    }

    @Property
    void varLongsAndFixedWidthIntegersRoundTrip(@ForAll long varLong, @ForAll int fixedInt, @ForAll long fixedLong) {
        ByteWriter out = new ByteWriter(1);
        out.writeVarLong(varLong);
        out.writeInt(fixedInt);
        out.writeLong(fixedLong);

        ByteReader in = new ByteReader(out.toByteArray(), "test");
        assertThat(in.readVarLong()).isEqualTo(varLong);
        assertThat(in.readInt()).isEqualTo(fixedInt);
        assertThat(in.readLong()).isEqualTo(fixedLong);
    }

    @Test
    void smallVarIntsTakeOneByte() {
        ByteWriter out = new ByteWriter(8);
        out.writeVarInt(127);
        assertThat(out.size()).isEqualTo(1);
        out.writeVarInt(128);
        assertThat(out.size()).isEqualTo(3);
    }

    @Test
    void integersAreBigEndian() {
        ByteWriter out = new ByteWriter(4);
        out.writeInt(0x01020304);

        assertThat(out.toByteArray()).containsExactly(1, 2, 3, 4);
    }

    @Test
    void putIntPatchesReservedBytes() {
        ByteWriter out = new ByteWriter(2);
        int at = out.skip(4);
        out.writeByte(9);
        out.putInt(at, 7);

        assertThat(out.toByteArray()).containsExactly(0, 0, 0, 7, 9);
    }

    @Test
    void truncateDropsTheTail() {
        ByteWriter out = new ByteWriter(4);
        out.writeInt(1);
        out.writeInt(2);
        out.truncate(4);

        assertThat(out.toByteArray()).containsExactly(0, 0, 0, 1);
        assertThatThrownBy(() -> out.truncate(5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readingPastTheEndIsCorruption() {
        ByteReader in = new ByteReader(new byte[] {1, 2}, "two bytes");

        assertThatThrownBy(in::readInt)
                .isInstanceOf(CorruptionException.class)
                .hasMessageContaining("two bytes");
    }

    @Test
    void aLengthBeyondTheDataIsCorruption() {
        ByteWriter out = new ByteWriter(4);
        out.writeVarInt(10);
        out.writeByte(1);

        assertThatThrownBy(() -> new ByteReader(out.toByteArray(), "test").readLength())
                .isInstanceOf(CorruptionException.class);
    }

    @Test
    void overlongVarIntsAreCorruption() {
        byte[] endless = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 1};

        assertThatThrownBy(() -> new ByteReader(endless, "test").readVarInt())
                .isInstanceOf(CorruptionException.class);
    }
}
