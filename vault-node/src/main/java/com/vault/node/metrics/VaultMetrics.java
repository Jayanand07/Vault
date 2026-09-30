package com.vault.node.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class VaultMetrics {

    private final Counter writesTotal;
    private final Counter readsTotal;
    private final Counter repairsTotal;
    private final Counter replicationFailuresTotal;
    private final Counter electionsTotal;

    public VaultMetrics(MeterRegistry registry) {
        this.writesTotal = Counter.builder("vault.writes.total")
                .description("Total number of object write operations")
                .register(registry);
                
        this.readsTotal = Counter.builder("vault.reads.total")
                .description("Total number of object read operations")
                .register(registry);
                
        this.repairsTotal = Counter.builder("vault.repairs.total")
                .description("Total number of read-repair operations triggered")
                .register(registry);
                
        this.replicationFailuresTotal = Counter.builder("vault.replication.failures")
                .description("Total number of replication failures")
                .register(registry);
                
        this.electionsTotal = Counter.builder("vault.elections.total")
                .description("Total number of leader elections triggered")
                .register(registry);
    }

    public void incrementWrites() { writesTotal.increment(); }
    public void incrementReads() { readsTotal.increment(); }
    public void incrementRepairs() { repairsTotal.increment(); }
    public void incrementReplicationFailures() { replicationFailuresTotal.increment(); }
    public void incrementElections() { electionsTotal.increment(); }
}
