# chunking-on-single-node

```json
{
  "connections": 6041,
  "connectSeconds": 5.5,
  "seconds": 45,
  "latencyMs": {
    "big-room": {
      "count": 3653474,
      "p50": 9118.41,
      "p95": 16192.88,
      "p99": 16847.07,
      "max": 17016.18,
      "mean": 9343.9
    },
    "probe-rooms": {
      "count": 4512,
      "p50": 17.13,
      "p95": 25.45,
      "p99": 32.92,
      "max": 107.26,
      "mean": 17.71
    }
  },
  "clientMsgsInPerSec": 81222,
  "clientEphInPerSec": 0,
  "publishedPerSec": 120.3,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 0,
  "closeCodes": {},
  "publishErrors": 0,
  "loadgenRssMb": 420,
  "server": {
    "nodes": 1,
    "msgsOutPerSec": 81268,
    "msgsOutByKindPerSec": {
      "durable": 81148,
      "ephemeral": 0,
      "presence": 0,
      "control": 120,
      "history": 0
    },
    "msgsInPerSec": 120,
    "eventLoopP99MsMax": 11.74,
    "eluAvg": 1,
    "cpuPercentAvg": 92.2,
    "heapMbMax": 88.5,
    "rssMbMax": 219.9,
    "fanoutP50Ms": 1000,
    "fanoutP99Ms": 1000,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 0,
    "connections": 6041,
    "resumeMessages": 0
  }
}
```
