# chunking-off-single-node

```json
{
  "connections": 6041,
  "connectSeconds": 5.5,
  "seconds": 45.7,
  "latencyMs": {
    "big-room": {
      "count": 3549013,
      "p50": 3008.22,
      "p95": 5342.13,
      "p99": 6259.16,
      "max": 6383.95,
      "mean": 2752.53
    },
    "probe-rooms": {
      "count": 4540,
      "p50": 3129.75,
      "p95": 6016.11,
      "p99": 6910.61,
      "max": 8353.96,
      "mean": 3113.51
    }
  },
  "clientMsgsInPerSec": 77830,
  "clientEphInPerSec": 0,
  "publishedPerSec": 118.9,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 2,
  "closeCodes": {
    "4029": 2
  },
  "publishErrors": 130,
  "loadgenRssMb": 436,
  "server": {
    "nodes": 1,
    "msgsOutPerSec": 77220,
    "msgsOutByKindPerSec": {
      "durable": 77107,
      "ephemeral": 0,
      "presence": 0,
      "control": 112,
      "history": 0
    },
    "msgsInPerSec": 116,
    "eventLoopP99MsMax": 4364.66,
    "eluAvg": 0.998,
    "cpuPercentAvg": 80.8,
    "heapMbMax": 100.7,
    "rssMbMax": 202.3,
    "fanoutP50Ms": 0.057,
    "fanoutP99Ms": 133.375,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 2,
    "connections": 6041,
    "resumeMessages": 0
  }
}
```
