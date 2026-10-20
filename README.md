1. Replicated key-value store with a failure-injection test suite (node kill, partition, slow disk) that checks linearizability on small histories.

2. Result:
   Acceptance: not measured  
   Performance: not measured  
   Correctness: not measured

3. Architecture:
```mermaid
flowchart LR
    Specification --> AwaitingWaveApproval --> Implementation
```

4. Run it:
No runnable implementation yet. This is a planned Wave 3 project.

5. Decisions and limits: Public repository reserved for independent work using public or synthetic data. Implementation has not started. No company affiliation or usage is claimed. Acceptance check: Test suite finds a seeded bug; README lists guarantees and what is not guaranteed.

6. Next: Implement after approval for Wave 3, then run and publish the required acceptance commands.
