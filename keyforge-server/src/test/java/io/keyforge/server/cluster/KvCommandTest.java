package io.keyforge.server.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import io.keyforge.storage.WriteCondition;

class KvCommandTest {

    @Property(tries = 300)
    void everyCommandSurvivesTheLogEncoding(@ForAll("commands") KvCommand command) {
        byte[] encoded = KvCommand.encode(command);
        KvCommand decoded = KvCommand.decode(encoded);
        assertThat(decoded.getClass()).isEqualTo(command.getClass());
        assertThat(decoded.time()).isEqualTo(command.time());
        assertThat(KvCommand.encode(decoded)).isEqualTo(encoded);
    }

    @Property(tries = 50)
    void trailingGarbageIsRejected(@ForAll("commands") KvCommand command) {
        byte[] encoded = KvCommand.encode(command);
        byte[] longer = Arrays.copyOf(encoded, encoded.length + 1);
        assertThatThrownBy(() -> KvCommand.decode(longer)).isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<KvCommand> commands() {
        Arbitrary<byte[]> bytes = Arbitraries.bytes().array(byte[].class).ofMaxSize(40);
        Arbitrary<Long> times = Arbitraries.longs().between(0, Long.MAX_VALUE);
        Arbitrary<List<byte[]>> keys = bytes.list().ofMaxSize(6);
        Arbitrary<KvCommand> set = Combinators.combine(times, bytes, bytes, Arbitraries.longs(),
                Arbitraries.of(WriteCondition.class)).as(KvCommand.Set::new);
        Arbitrary<KvCommand> setAll = Combinators.combine(times, keys).as((time, list) ->
                new KvCommand.SetAll(time, list, list.stream().map(key -> new byte[] {(byte) key.length}).toList()));
        Arbitrary<KvCommand> increment = Combinators.combine(times, bytes, Arbitraries.longs())
                .as(KvCommand.IncrementBy::new);
        Arbitrary<KvCommand> delete = Combinators.combine(times, keys).as(KvCommand.Delete::new);
        Arbitrary<KvCommand> clear = times.map(KvCommand.Clear::new);
        return Arbitraries.oneOf(set, setAll, increment, delete, clear);
    }
}
