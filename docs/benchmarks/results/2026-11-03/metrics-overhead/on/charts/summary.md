### redis-benchmark: throughput

Requests per second (closed loop). Server CPU is the time all of the target's server containers spent per request.

| Target | Pipeline | SET | GET | INCR | Server CPU per request (SET / GET / INCR) |
| --- | ---: | ---: | ---: | ---: | ---: |
| KeyForge, memory | 1 | 117,578 | 105,141 | 108,061 | 14.6 / 15.5 / 15.4 µs |
| KeyForge, memory | 16 | 768,521 | 712,962 | 624,376 | 2.5 / 2.7 / 3.0 µs |

### redis-benchmark: closed-loop latency

redis-benchmark's own p50 / p99 in ms. A closed loop waits for each reply before sending the next request, so these hide stalls (coordinated omission); see the open-loop table.

| Target | Pipeline | SET p50 / p99 | GET p50 / p99 | INCR p50 / p99 |
| --- | ---: | ---: | ---: | ---: |
| KeyForge, memory | 1 | 0.215 / 0.887 | 0.215 / 1.623 | 0.207 / 1.679 |
| KeyForge, memory | 16 | 0.431 / 12.391 | 0.487 / 13.055 | 0.575 / 14.751 |

### Load generator: open-loop latency

Latency in ms from each request's intended send time (no coordinated omission), 80% GET / 20% SET. *Median 1-s p99* is the median of the per-second p99s: the tail of a typical second, which an isolated stall cannot move.

| Target | Offered (req/s) | Delivered (req/s) | p50 | p99 | p99.9 | p99.99 | max | Median 1-s p99 | Server CPU (cores) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| KeyForge, memory | 20,000 | 20,001 | 0.147 | 60.8 | 257 | 288 | 299 | 2.46 | 0.76 | 0 |
