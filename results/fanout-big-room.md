# fanout-big-room

```json
{
  "connections": 6041,
  "connectSeconds": 6,
  "seconds": 60.1,
  "latencyMs": {
    "big-room": {
      "count": 1800000,
      "p50": 22.6,
      "p95": 37.07,
      "p99": 60.82,
      "max": 122.96,
      "mean": 21.93
    },
    "probe-rooms": {
      "count": 6008,
      "p50": 0.84,
      "p95": 29.23,
      "p99": 36.35,
      "max": 99.29,
      "mean": 6.6
    }
  },
  "clientMsgsInPerSec": 30075,
  "clientEphInPerSec": 0,
  "publishedPerSec": 105,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 0,
  "closeCodes": {},
  "publishErrors": 0,
  "loadgenRssMb": 241,
  "server": {
    "nodes": 3,
    "msgsOutPerSec": 30180,
    "msgsOutByKindPerSec": {
      "durable": 30075,
      "ephemeral": 0,
      "presence": 0,
      "control": 105,
      "history": 0
    },
    "msgsInPerSec": 105,
    "eventLoopP99MsMax": 20.67,
    "eluAvg": 0.127,
    "cpuPercentAvg": 10.2,
    "heapMbMax": 55.1,
    "rssMbMax": 133,
    "fanoutP50Ms": 0.058,
    "fanoutP99Ms": 24.856,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 0,
    "connections": 6041,
    "resumeMessages": 0
  }
}
```
