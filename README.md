1. Replicated key-value store with a failure-injection test suite (node kill, partition, slow disk) that checks linearizability on small histories.

2. Result:
   Replica processes: 3  
   History operations checked: 6  
   Fault scenarios passed: 3  

   Measured with `make bench`: [raw results](evidence/bench.json), [command output](evidence/checks.log), [environment](evidence/environment.json).

3. Architecture:
```mermaid
flowchart LR
    Client --> FixedCoordinator --> ReplicaProcesses --> QuorumPrepare --> SmallHistoryChecker
```

4. Run it:
```sh
make setup
make lint test bench
.venv/bin/python src/main.py --seed-bug  # expected exit 1
```

5. Decisions and limits: Three actual child processes hold in-memory replicas. Fixed coordinator serializes client operations; successful writes require two prepare acknowledgements. Tests physically kill one replica and inject transport partition/slow-replica acknowledgements. No leader election, coordinator crash recovery, disk durability, automatic repair, Byzantine tolerance or production consensus guarantee is claimed. Replicas prepare values before coordinator commit; the coordinator is the authoritative read path. Small completed histories are checked exhaustively against single-register semantics; failed operations are excluded because they do not update coordinator state. Checker complexity is factorial. The seeded stale-read history fails in evidence/gate-failure.log.

6. Next: Add durable logs, repair, leader election and concurrent histories with timeouts/indeterminate operations.
