package com.example.loadbalancer.service.schedulingalgorithms;

import com.example.loadbalancer.service.BackendLoadTracker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance-based dynamic scheduling: least in-flight requests.
 *
 * The load of a backend is the number of requests currently outstanding on it.
 * Selection rules, in order:
 *   1. skip backends that failed the health check or were ejected after a failed forward;
 *   2. skip backends whose in-flight count has reached the threshold (overloaded);
 *   3. among the rest pick the one with the fewest in-flight requests;
 *   4. ties are broken round-robin so equally loaded backends receive equal traffic.
 * If every backend is over the threshold the least loaded available one is used,
 * and if none is available at all the least loaded overall is used rather than failing.
 *
 * Cost per request: one scan over the backend list and no allocation.
 */
@Component
@Primary
public class LeastLoadedScheduling implements SchedulingAlgorithm {

    private final List<String> servers;
    private final BackendLoadTracker tracker;
    private final int maxInflightPerBackend;
    private final AtomicInteger cursor = new AtomicInteger();

    public LeastLoadedScheduling(BackendLoadTracker tracker,
                                 @Value("${lb.max-inflight-per-backend:200}") int maxInflightPerBackend) {
        this.servers = tracker.servers();
        this.tracker = tracker;
        this.maxInflightPerBackend = maxInflightPerBackend;
    }

    @Override
    public String getServer(String clientKey) {
        int n = servers.size();
        if (n == 0) return null;

        long now = System.currentTimeMillis();
        int start = Math.floorMod(cursor.getAndIncrement(), n);

        String best = null;
        String bestAvailable = null;
        String bestAny = null;
        int bestLoad = Integer.MAX_VALUE;
        int bestAvailableLoad = Integer.MAX_VALUE;
        int bestAnyLoad = Integer.MAX_VALUE;

        for (int i = 0; i < n; i++) {
            String server = servers.get((start + i) % n);
            int load = tracker.inflight(server);

            if (load < bestAnyLoad) {
                bestAny = server;
                bestAnyLoad = load;
            }
            if (!tracker.isAvailable(server, now)) continue;

            if (load < bestAvailableLoad) {
                bestAvailable = server;
                bestAvailableLoad = load;
            }
            if (load >= maxInflightPerBackend) continue;

            if (load < bestLoad) {
                best = server;
                bestLoad = load;
            }
        }

        if (best != null) return best;
        if (bestAvailable != null) return bestAvailable;
        return bestAny;
    }
}
