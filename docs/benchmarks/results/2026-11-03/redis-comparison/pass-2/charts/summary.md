### redis-benchmark: throughput

Requests per second (closed loop). Server CPU is the time all of the target's server containers spent per request.

| Target | Pipeline | SET | GET | INCR | Server CPU per request (SET / GET / INCR) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Redis | 1 | 79,898 | 60,430 | 83,285 | 12.2 / 15.0 / 11.8 µs |
| Redis, AOF everysec | 1 | 78,394 | 86,911 | 81,599 | 12.9 / 11.4 / 12.2 µs |
| KeyForge, memory | 1 | 111,049 | 114,181 | 121,153 | 14.6 / 14.3 / 14.1 µs |
| KeyForge, LSM | 1 | 28,360 | 84,631 | 25,967 | 55.8 / 20.2 / 59.8 µs |
| KeyForge, LSM, virtual threads | 1 | 72,701 | 121,168 | 67,755 | 23.7 / 15.7 / 23.6 µs |
| KeyForge 3 nodes, leader | 1 | 1,931 | 11,976 | 2,921 | 591.6 / 154.3 / 353.8 µs |
| KeyForge 3 nodes, follower | 1 | 1,098 | 4,437 | 1,535 | 898.7 / 399.6 / 532.0 µs |
| Redis | 16 | 644,413 | 766,049 | 768,758 | 1.6 / 1.3 / 1.3 µs |
| Redis, AOF everysec | 16 | 475,647 | 799,744 | 605,254 | 2.3 / 1.3 / 1.7 µs |
| KeyForge, memory | 16 | 951,837 | 740,521 | 713,776 | 2.0 / 2.6 / 2.7 µs |
| KeyForge, LSM | 16 | 55,782 | 288,650 | 49,671 | 23.5 / 6.6 / 26.5 µs |
| KeyForge, LSM, virtual threads | 16 | 184,939 | 392,003 | 145,756 | 8.3 / 4.8 / 10.2 µs |
| KeyForge 3 nodes, leader | 16 | 3,149 | 29,940 | 3,326 | 334.0 / 57.8 / 329.5 µs |
| KeyForge 3 nodes, follower | 16 | 1,685 | 7,483 | 2,394 | 509.6 / 235.8 / 413.8 µs |

### redis-benchmark: closed-loop latency

redis-benchmark's own p50 / p99 in ms. A closed loop waits for each reply before sending the next request, so these hide stalls (coordinated omission); see the open-loop table.

| Target | Pipeline | SET p50 / p99 | GET p50 / p99 | INCR p50 / p99 |
| --- | ---: | ---: | ---: | ---: |
| Redis | 1 | 0.495 / 1.367 | 0.495 / 6.207 | 0.479 / 1.575 |
| Redis, AOF everysec | 1 | 0.543 / 1.191 | 0.495 / 1.167 | 0.527 / 1.199 |
| KeyForge, memory | 1 | 0.215 / 0.999 | 0.215 / 0.999 | 0.207 / 0.847 |
| KeyForge, LSM | 1 | 1.383 / 5.471 | 0.207 / 6.519 | 1.583 / 5.567 |
| KeyForge, LSM, virtual threads | 1 | 0.543 / 2.655 | 0.255 / 0.943 | 0.599 / 1.895 |
| KeyForge 3 nodes, leader | 1 | 20.767 / 69.439 | 2.735 / 21.167 | 13.807 / 99.775 |
| KeyForge 3 nodes, follower | 1 | 35.391 / 267.775 | 9.159 / 30.031 | 23.471 / 205.055 |
| Redis | 16 | 1.039 / 10.079 | 0.887 / 1.631 | 0.903 / 1.887 |
| Redis, AOF everysec | 16 | 1.599 / 4.839 | 0.879 / 1.551 | 1.127 / 2.279 |
| KeyForge, memory | 16 | 0.367 / 10.727 | 0.479 / 13.391 | 0.527 / 13.319 |
| KeyForge, LSM | 16 | 13.039 / 34.271 | 0.951 / 25.151 | 14.359 / 46.623 |
| KeyForge, LSM, virtual threads | 16 | 3.655 / 14.967 | 1.743 / 11.887 | 4.271 / 37.279 |
| KeyForge 3 nodes, leader | 16 | 231.807 / 733.183 | 19.391 / 74.943 | 226.943 / 314.623 |
| KeyForge 3 nodes, follower | 16 | 359.423 / 1794.047 | 94.975 / 211.455 | 279.039 / 1524.735 |

### Load generator: open-loop latency

Latency in ms from each request's intended send time (no coordinated omission), 80% GET / 20% SET. *Median 1-s p99* is the median of the per-second p99s: the tail of a typical second, which an isolated stall cannot move.

| Target | Offered (req/s) | Delivered (req/s) | p50 | p99 | p99.9 | p99.99 | max | Median 1-s p99 | Server CPU (cores) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| KeyForge 3 nodes, leader | 500 | 500 | 2.93 | 23.6 | 38.6 | 44.6 | 54.0 | 17.3 | 0.29 | 0 |
| KeyForge 3 nodes, follower | 500 | 500 | 6.52 | 758 | 989 | 1,051 | 1,056 | 34.3 | 0.39 | 0 |
| KeyForge 3 nodes, leader | 1,000 | 1,000 | 6.50 | 1,876 | 2,002 | 2,030 | 2,037 | 36.9 | 0.32 | 0 |
| KeyForge 3 nodes, follower | 1,000 | 958 | 8.01 | 1,023 | 1,282 | 1,338 | 1,341 | 28.5 | 0.41 | 0 |
| Redis | 20,000 | 20,000 | 0.161 | 5.44 | 14.8 | 23.5 | 27.5 | 0.647 | 0.63 | 0 |
| Redis, AOF everysec | 20,000 | 20,000 | 0.165 | 2.40 | 15.5 | 23.2 | 27.8 | 0.961 | 0.65 | 0 |
| KeyForge, memory | 20,000 | 20,001 | 0.168 | 35.6 | 88.7 | 124 | 143 | 7.26 | 0.85 | 0 |
| KeyForge, LSM | 20,000 | 19,999 | 0.161 | 8.72 | 21.1 | 25.1 | 29.7 | 4.06 | 0.96 | 0 |
| KeyForge, LSM, virtual threads | 20,000 | 20,002 | 0.192 | 6.31 | 22.5 | 29.8 | 38.5 | 2.87 | 1.12 | 0 |
| Redis | 60,000 | 59,995 | 0.271 | 121 | 170 | 187 | 194 | 4.33 | 0.98 | 0 |
| Redis, AOF everysec | 60,000 | 60,001 | 0.287 | 12.6 | 21.0 | 25.6 | 29.9 | 5.56 | 0.99 | 0 |
| KeyForge, memory | 60,000 | 60,007 | 0.121 | 14.1 | 29.0 | 45.2 | 64.4 | 9.23 | 1.43 | 0 |
| KeyForge, LSM | 60,000 | 60,810 | 2.46 | 6,375 | 6,895 | 6,979 | 7,160 | 6,365 | 1.88 | 0 |
| KeyForge, LSM, virtual threads | 60,000 | 66,283 | 1,085 | 3,163 | 3,324 | 3,362 | 3,370 | 1,416 | 1.91 | 0 |
