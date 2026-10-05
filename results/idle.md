# idle

```json
{
  "connections": 9000,
  "connectErrors": 0,
  "connectSeconds": 33.7,
  "connectLatencyMs": {
    "count": 9000,
    "p50": 2.27,
    "p95": 4.73,
    "p99": 10.65,
    "max": 70.2,
    "mean": 2.71
  },
  "steps": [
    {
      "connections": 3000,
      "heapMb": 22.4,
      "rssMb": 65,
      "heapPerConnKb": 7.66,
      "rssPerConnKb": 22.18,
      "seconds": 3.2
    },
    {
      "connections": 6000,
      "heapMb": 36,
      "rssMb": 40.9,
      "heapPerConnKb": 6.15,
      "rssPerConnKb": 6.99,
      "seconds": 3.2
    },
    {
      "connections": 9000,
      "heapMb": 54.5,
      "rssMb": 61.5,
      "heapPerConnKb": 6.2,
      "rssPerConnKb": 6.99,
      "seconds": 3.2
    }
  ],
  "heapPerConnKb": 6.2,
  "rssPerConnKb": 6.99,
  "heapPerConnSlopeKb": 5.48,
  "idleCpuPercentPerNode": 1.22,
  "nodeHeapMbTotal": 93.2,
  "nodeRssMbTotal": 244.6,
  "server": {
    "nodes": 3,
    "msgsOutPerSec": 0,
    "msgsOutByKindPerSec": {
      "durable": 0,
      "ephemeral": 0,
      "presence": 0,
      "control": 0,
      "history": 0
    },
    "msgsInPerSec": 0,
    "eventLoopP99MsMax": 22.95,
    "eluAvg": 0.017,
    "cpuPercentAvg": 1.2,
    "heapMbMax": 31.7,
    "rssMbMax": 98.8,
    "fanoutP50Ms": 0,
    "fanoutP99Ms": 0,
    "slowConsumerDisconnects": 0,
    "ephemeralDropped": 0,
    "upgradesOk": 0,
    "connections": 9000,
    "resumeMessages": 0
  }
}
```
