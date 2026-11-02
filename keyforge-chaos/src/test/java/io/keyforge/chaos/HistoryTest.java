package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class HistoryTest {

    private final AtomicLong clock = new AtomicLong(1_000);
    private final History history = new History(() -> clock.addAndGet(10));

    @Test
    void recordsInvocationsAndCompletionsInOrderWithGrowingTimestamps() {
        HistoryEvent.Op set = history.invoke(3, OpFunction.SET, "k", 1000L, "kf1");
        HistoryEvent.Op get = history.invoke(4, OpFunction.GET, "k", null, "kf2");
        HistoryEvent.Op setDone = history.complete(set, EventType.OK, null, "ignored for ok");
        HistoryEvent.Op getDone = history.complete(get, EventType.INFO, null, "timed out after 10000 ms");

        assertThat(history.events()).containsExactly(set, get, setDone, getDone);
        assertThat(history.events()).extracting(HistoryEvent::index).containsExactly(0L, 1L, 2L, 3L);
        assertThat(history.events()).extracting(HistoryEvent::timeNanos).isSorted().doesNotHaveDuplicates();
        assertThat(set.timeNanos()).as("time starts at the history's creation").isEqualTo(10);
        assertThat(setDone.value()).as("a set's completion repeats the value written").isEqualTo(1000L);
        assertThat(setDone.error()).isNull();
        assertThat(getDone).extracting(HistoryEvent.Op::type, HistoryEvent.Op::process, HistoryEvent.Op::node,
                HistoryEvent.Op::error).containsExactly(EventType.INFO, 4, "kf2", "timed out after 10000 ms");
    }

    @Test
    void onlyInvocationsCanBeCompleted() {
        HistoryEvent.Op invocation = history.invoke(0, OpFunction.GET, "k", null, "kf1");
        HistoryEvent.Op completion = history.complete(invocation, EventType.OK, 5L, null);

        assertThatThrownBy(() -> history.complete(completion, EventType.OK, 5L, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> history.complete(invocation, EventType.INVOKE, 5L, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesTheFormatTheCheckerReads() throws IOException {
        HistoryEvent.Op incr = history.invoke(0, OpFunction.INCR, "lin:1", null, "kf1");
        history.nemesis(EventType.INVOKE, "start", FaultKind.PARTITION, "halves {kf1} | {kf2}");
        history.complete(incr, EventType.OK, 7L, null);
        HistoryEvent.Op get = history.invoke(1, OpFunction.GET, "lin:1", null, "kf2");
        history.complete(get, EventType.FAIL, null, "CLUSTERDOWN no \"leader\"\n");
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("seed", "42");
        metadata.put("nemesis", "partition");
        StringWriter out = new StringWriter();

        history.writeJson(out, metadata);

        assertThat(out.toString()).isEqualTo("""
                {
                  "format": "keyforge-history/1",
                  "started": "%s",
                  "time_unit": "ns",
                  "metadata": {
                    "seed": "42",
                    "nemesis": "partition"
                  },
                  "events": [
                    {"index":0,"time":10,"type":"invoke","process":0,"f":"incr","key":"lin:1","value":null,"node":"kf1"},
                    {"index":1,"time":20,"type":"invoke","process":"nemesis","f":"start","fault":"partition","value":"halves {kf1} | {kf2}"},
                    {"index":2,"time":30,"type":"ok","process":0,"f":"incr","key":"lin:1","value":7,"node":"kf1"},
                    {"index":3,"time":40,"type":"invoke","process":1,"f":"get","key":"lin:1","value":null,"node":"kf2"},
                    {"index":4,"time":50,"type":"fail","process":1,"f":"get","key":"lin:1","value":null,"node":"kf2","error":"CLUSTERDOWN no \\"leader\\"\\n"}
                  ]
                }
                """.formatted(history.started()));
    }

    @Test
    void anEmptyHistoryIsValidJson() throws IOException {
        StringWriter out = new StringWriter();

        history.writeJson(out, Map.of());

        assertThat(out.toString()).contains("\"metadata\": {},\n").endsWith("\"events\": []\n}\n");
    }

    @Test
    void quotesControlCharactersAndUnicode() {
        assertThat(Json.quote("a\tb\u0001c\\d\"é")).isEqualTo("\"a\\tb\\u0001c\\\\d\\\"é\"");
        assertThat(Json.quote(null)).isEqualTo("null");
        assertThat(List.of(Json.quote(""), Json.quote("\r"))).containsExactly("\"\"", "\"\\r\"");
    }
}
