### redis-benchmark: throughput

Requests per second (closed loop). Server CPU is the time all of the target's server containers spent per request.

| Target | Pipeline | SET | GET | INCR | Server CPU per request (SET / GET / INCR) |
| --- | ---: | ---: | ---: | ---: | ---: |
| KeyForge, memory | 1 | 107,921 | 114,207 | 114,103 | 14.8 / 14.4 / 14.2 µs |
| KeyForge, memory | 16 | 999,600 | 799,488 | 767,342 | 2.0 / 2.5 / 2.5 µs |

### redis-benchmark: closed-loop latency

redis-benchmark's own p50 / p99 in ms. A closed loop waits for each reply before sending the next request, so these hide stalls (coordinated omission); see the open-loop table.

| Target | Pipeline | SET p50 / p99 | GET p50 / p99 | INCR p50 / p99 |
| --- | ---: | ---: | ---: | ---: |
| KeyForge, memory | 1 | 0.215 / 1.183 | 0.215 / 0.823 | 0.215 / 0.823 |
| KeyForge, memory | 16 | 0.335 / 10.879 | 0.455 / 11.807 | 0.415 / 10.599 |

### Load generator: open-loop latency

Latency in ms from each request's intended send time (no coordinated omission), 80% GET / 20% SET. *Median 1-s p99* is the median of the per-second p99s: the tail of a typical second, which an isolated stall cannot move.

| Target | Offered (req/s) | Delivered (req/s) | p50 | p99 | p99.9 | p99.99 | max | Median 1-s p99 | Server CPU (cores) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| KeyForge, memory | 20,000 | 20,000 | 0.143 | 1.43 | 11.2 | 21.8 | 28.4 | 0.904 | 0.70 | 0 |
