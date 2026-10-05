# fanout-haproxy-processes-run2

```json
{
  "connections": 3001,
  "connectSeconds": 3,
  "seconds": 30.1,
  "latencyMs": {
    "big-room": {
      "count": 450901,
      "p50": 33.58,
      "p95": 60.82,
      "p99": 80.26,
      "max": 189.12,
      "mean": 32.3
    }
  },
  "clientMsgsInPerSec": 14997,
  "clientEphInPerSec": 0,
  "publishedPerSec": 5,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 0,
  "closeCodes": {},
  "publishErrors": 0,
  "loadgenRssMb": 165,
  "server": {
    "nodes": 3,
    "msgsOutPerSec": 14981,
    "msgsOutByKindPerSec": {
      "durable": 14976,
      "ephemeral": 0,
      "presence": 0,
      "control": 5,
      "history": 0
    },
    "msgsInPerSec": 5,
    "eventLoopP99MsMax": 73.76,
    "eluAvg": 0.18,
    "cpuPercentAvg": 6.2,
    "heapMbMax": 48.4,
    "rssMbMax": 77.3,
    "fanoutP50Ms": 23.178,
    "fanoutP99Ms": 98.125,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 0,
    "connections": 3001,
    "resumeMessages": 0
  }
}
```
