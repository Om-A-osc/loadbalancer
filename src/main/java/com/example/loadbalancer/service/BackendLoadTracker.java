package com.example.loadbalancer.service;

import com.example.loadbalancer.health.HealthCheck;
import com.example.loadbalancer.service.registry.ServerRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live load and health state for every backend.
 *
 * Fed by the proxy path (one atomic increment before a forward and one
 * decrement after) and by a periodic active health check. Read by
 * LeastLoadedScheduling to pick a backend for each request.
 */
@Component
public class BackendLoadTracker {

    /** Mutable per-backend counters. */
    public static final class Backend {
        final AtomicInteger inflight = new AtomicInteger();
        final AtomicLong total = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        /** Approximate moving average for reporting; concurrent updates are a benign race. */
        volatile double ewmaLatencyMs = 0;
        volatile boolean healthy = true;
        volatile long downUntil = 0;
    }

    private final Map<String, Backend> backends;
    private final HealthCheck healthCheck;
    private final long healthIntervalMs;
    private final long ejectMs;

    public BackendLoadTracker(ServerRegistry registry,
                              HealthCheck healthCheck,
                              @Value("${lb.health-interval-ms:2000}") long healthIntervalMs,
                              @Value("${lb.eject-ms:3000}") long ejectMs) {
        Map<String, Backend> map = new LinkedHashMap<>();
        for (String server : registry.getServers()) {
            map.put(server.trim(), new Backend());
        }
        this.backends = Collections.unmodifiableMap(map);
        this.healthCheck = healthCheck;
        this.healthIntervalMs = healthIntervalMs;
        this.ejectMs = ejectMs;
    }

    public List<String> servers() {
        return List.copyOf(backends.keySet());
    }

    public void onStart(String server) {
        Backend b = backends.get(server);
        if (b == null) return;
        b.inflight.incrementAndGet();
        b.total.incrementAndGet();
    }

    public void onComplete(String server, double latencyMs, boolean success) {
        Backend b = backends.get(server);
        if (b == null) return;
        b.inflight.decrementAndGet();
        if (!success) b.failures.incrementAndGet();
        double previous = b.ewmaLatencyMs;
        b.ewmaLatencyMs = previous == 0 ? latencyMs : previous * 0.9 + latencyMs * 0.1;
    }

    /** Passive detection: a failed forward takes the backend out of rotation briefly. */
    public void markDown(String server) {
        Backend b = backends.get(server);
        if (b != null) b.downUntil = System.currentTimeMillis() + ejectMs;
    }

    public int inflight(String server) {
        Backend b = backends.get(server);
        return b == null ? Integer.MAX_VALUE : b.inflight.get();
    }

    public boolean isAvailable(String server, long now) {
        Backend b = backends.get(server);
        return b != null && b.healthy && now >= b.downUntil;
    }

    /** Active detection: poll /health on every backend every healthIntervalMs. */
    @PostConstruct
    void startHealthLoop() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    Set<String> healthy = new HashSet<>(healthCheck.getHealthyServers());
                    backends.forEach((server, b) -> b.healthy = healthy.contains(server));
                } catch (Exception ignored) {
                    // keep the last known state
                }
                try {
                    Thread.sleep(healthIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "lb-health-loop");
        thread.setDaemon(true);
        thread.start();
    }

    public Map<String, Object> snapshot() {
        long now = System.currentTimeMillis();
        Map<String, Object> out = new LinkedHashMap<>();
        backends.forEach((server, b) -> {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("inflight", b.inflight.get());
            s.put("total", b.total.get());
            s.put("failures", b.failures.get());
            s.put("ewmaLatencyMs", Math.round(b.ewmaLatencyMs * 100.0) / 100.0);
            s.put("healthy", b.healthy);
            s.put("ejected", now < b.downUntil);
            out.put(server, s);
        });
        return out;
    }
}
