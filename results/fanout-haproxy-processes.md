# fanout-haproxy-processes

```json
{
  "connections": 3001,
  "connectSeconds": 3,
  "seconds": 30.1,
  "latencyMs": {
    "big-room": {
      "count": 450588,
      "p50": 33.58,
      "p95": 184.37,
      "p99": 440.65,
      "max": 659.63,
      "mean": 50.63
    }
  },
  "clientMsgsInPerSec": 14985,
  "clientEphInPerSec": 0,
  "publishedPerSec": 5,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 0,
  "closeCodes": {},
  "publishErrors": 0,
  "loadgenRssMb": 134,
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
    "eventLoopP99MsMax": 309.03,
    "eluAvg": 0.212,
    "cpuPercentAvg": 5.3,
    "heapMbMax": 46.9,
    "rssMbMax": 70.6,
    "fanoutP50Ms": 19.96,
    "fanoutP99Ms": 275,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 0,
    "connections": 3001,
    "resumeMessages": 0
  }
}
```
