# slow-consumers

```json
{
  "rooms": 177,
  "slowClients": 50,
  "connections": 1000,
  "connectSeconds": 1,
  "seconds": 90.2,
  "latencyMs": {
    "normal": {
      "count": 572177,
      "p50": 7.03,
      "p95": 90.38,
      "p99": 243.27,
      "max": 455.25,
      "mean": 19.61
    },
    "slow": {
      "count": 997,
      "p50": 37199,
      "p95": 38701.84,
      "p99": 41002.09,
      "max": 41002.09,
      "mean": 36242.12
    }
  },
  "clientMsgsInPerSec": 6356,
  "clientEphInPerSec": 52541,
  "publishedPerSec": 969.5,
  "gaps": 0,
  "duplicates": 0,
  "resets": 0,
  "resumedMessages": 0,
  "reconnects": 0,
  "closeCodes": {
    "1006": 50
  },
  "publishErrors": 0,
  "loadgenRssMb": 188,
  "server": {
    "nodes": 3,
    "msgsOutPerSec": 60697,
    "msgsOutByKindPerSec": {
      "durable": 6459,
      "ephemeral": 53273,
      "presence": 3,
      "control": 962,
      "history": 0
    },
    "msgsInPerSec": 10462,
    "eventLoopP99MsMax": 244.28,
    "eluAvg": 0.675,
    "cpuPercentAvg": 52.8,
    "heapMbMax": 73.4,
    "rssMbMax": 200,
    "fanoutP50Ms": 0.051,
    "fanoutP99Ms": 0.249,
    "slowConsumerDisconnects": 2,
    "ephemeralDropped": 14956,
    "upgradesOk": 0,
    "connections": 950,
    "resumeMessages": 0
  }
}
```
