### redis-benchmark: throughput

Requests per second (closed loop). Server CPU is the time all of the target's server containers spent per request.

| Target | Pipeline | SET | GET | INCR | Server CPU per request (SET / GET / INCR) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Redis | 1 | 88,857 | 86,889 | 88,889 | 11.1 / 11.4 / 11.2 µs |
| Redis, AOF everysec | 1 | 76,917 | 86,934 | 83,326 | 13.0 / 11.5 / 12.0 µs |
| KeyForge, memory | 1 | 114,273 | 111,049 | 117,578 | 14.7 / 14.9 / 14.4 µs |
| KeyForge, LSM | 1 | 27,964 | 114,234 | 25,913 | 56.6 / 16.0 / 60.5 µs |
| KeyForge, LSM, virtual threads | 1 | 74,047 | 114,247 | 67,737 | 24.3 / 16.2 / 24.3 µs |
| KeyForge 3 nodes, leader | 1 | 2,443 | 13,316 | 2,993 | 616.4 / 130.6 / 350.3 µs |
| KeyForge 3 nodes, follower | 1 | 2,101 | 4,437 | 2,661 | 775.1 / 415.7 / 485.5 µs |
| Redis | 16 | 713,878 | 768,640 | 799,488 | 1.4 / 1.3 / 1.2 µs |
| Redis, AOF everysec | 16 | 486,950 | 768,758 | 624,454 | 2.3 / 1.3 / 1.7 µs |
| KeyForge, memory | 16 | 951,837 | 799,872 | 689,275 | 2.0 / 2.5 / 2.8 µs |
| KeyForge, LSM | 16 | 56,263 | 363,214 | 48,940 | 23.4 / 5.3 / 27.0 µs |
| KeyForge, LSM, virtual threads | 16 | 186,651 | 320,184 | 137,661 | 8.2 / 5.8 / 10.8 µs |
| KeyForge 3 nodes, leader | 16 | 3,326 | 29,910 | 2,721 | 331.7 / 59.9 / 336.4 µs |
| KeyForge 3 nodes, follower | 16 | 2,444 | 7,979 | 2,850 | 431.4 / 213.7 / 396.4 µs |

### redis-benchmark: closed-loop latency

redis-benchmark's own p50 / p99 in ms. A closed loop waits for each reply before sending the next request, so these hide stalls (coordinated omission); see the open-loop table.

| Target | Pipeline | SET p50 / p99 | GET p50 / p99 | INCR p50 / p99 |
| --- | ---: | ---: | ---: | ---: |
| Redis | 1 | 0.471 / 1.063 | 0.479 / 1.231 | 0.479 / 1.135 |
| Redis, AOF everysec | 1 | 0.559 / 1.231 | 0.487 / 1.167 | 0.519 / 1.159 |
| KeyForge, memory | 1 | 0.215 / 0.959 | 0.207 / 1.247 | 0.207 / 1.015 |
| KeyForge, LSM | 1 | 1.415 / 5.535 | 0.199 / 3.087 | 1.607 / 5.615 |
| KeyForge, LSM, virtual threads | 1 | 0.519 / 2.663 | 0.295 / 1.199 | 0.599 / 2.039 |
| KeyForge 3 nodes, leader | 1 | 16.607 / 61.759 | 2.935 / 19.743 | 13.943 / 53.887 |
| KeyForge 3 nodes, follower | 1 | 21.103 / 67.711 | 9.263 / 32.223 | 17.503 / 40.159 |
| Redis | 16 | 1.007 / 2.391 | 0.879 / 1.703 | 0.847 / 1.679 |
| Redis, AOF everysec | 16 | 1.559 / 3.991 | 0.879 / 1.639 | 1.119 / 2.231 |
| KeyForge, memory | 16 | 0.351 / 10.863 | 0.463 / 12.287 | 0.551 / 13.447 |
| KeyForge, LSM | 16 | 12.871 / 35.423 | 0.903 / 21.695 | 14.487 / 45.695 |
| KeyForge, LSM, virtual threads | 16 | 3.607 / 15.855 | 1.759 / 18.431 | 4.559 / 32.111 |
| KeyForge 3 nodes, leader | 16 | 225.279 / 361.471 | 20.095 / 59.487 | 232.063 / 1121.279 |
| KeyForge 3 nodes, follower | 16 | 280.575 / 809.471 | 82.943 / 202.879 | 269.567 / 346.367 |

### Load generator: open-loop latency

Latency in ms from each request's intended send time (no coordinated omission), 80% GET / 20% SET. *Median 1-s p99* is the median of the per-second p99s: the tail of a typical second, which an isolated stall cannot move.

| Target | Offered (req/s) | Delivered (req/s) | p50 | p99 | p99.9 | p99.99 | max | Median 1-s p99 | Server CPU (cores) | Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| KeyForge 3 nodes, leader | 500 | 500 | 3.03 | 126 | 170 | 188 | 189 | 15.4 | 0.28 | 0 |
| KeyForge 3 nodes, follower | 500 | 500 | 5.19 | 58.8 | 116 | 141 | 146 | 29.3 | 0.41 | 0 |
| KeyForge 3 nodes, leader | 1,000 | 1,000 | 4.50 | 28.0 | 52.6 | 74.6 | 75.8 | 21.0 | 0.34 | 0 |
| KeyForge 3 nodes, follower | 1,000 | 1,000 | 6.39 | 49.7 | 118 | 134 | 143 | 26.8 | 0.43 | 0 |
| Redis | 20,000 | 20,000 | 0.165 | 0.593 | 4.64 | 12.1 | 15.5 | 0.449 | 0.59 | 0 |
| Redis, AOF everysec | 20,000 | 20,003 | 0.163 | 5.86 | 20.3 | 28.0 | 38.3 | 1.22 | 0.67 | 0 |
| KeyForge, memory | 20,000 | 20,000 | 0.135 | 4.50 | 19.3 | 27.3 | 31.2 | 2.72 | 0.77 | 0 |
| KeyForge, LSM | 20,000 | 20,000 | 0.167 | 9.48 | 27.2 | 41.6 | 52.0 | 4.44 | 0.95 | 0 |
| KeyForge, LSM, virtual threads | 20,000 | 19,954 | 1.09 | 269 | 419 | 488 | 508 | 69.4 | 1.25 | 0 |
| Redis | 60,000 | 60,001 | 0.231 | 6.07 | 17.5 | 25.5 | 29.0 | 2.18 | 0.99 | 0 |
| Redis, AOF everysec | 60,000 | 60,000 | 0.351 | 92.5 | 102 | 105 | 109 | 10.4 | 0.99 | 0 |
| KeyForge, memory | 60,000 | 59,951 | 0.125 | 18.8 | 47.8 | 64.9 | 82.8 | 7.69 | 1.44 | 0 |
| KeyForge, LSM | 60,000 | 61,918 | 31.8 | 10,620 | 12,163 | 12,281 | 12,298 | 9,106 | 1.90 | 0 |
| KeyForge, LSM, virtual threads | 60,000 | 61,058 | 14.3 | 596 | 791 | 903 | 929 | 97.8 | 1.83 | 0 |
