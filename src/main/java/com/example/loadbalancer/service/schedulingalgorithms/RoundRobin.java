package com.example.loadbalancer.service.schedulingalgorithms;

import com.example.loadbalancer.health.HealthCheck;
import com.example.loadbalancer.service.registry.ServerRegistry;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobin implements SchedulingAlgorithm {

    private volatile List<String> servers = Collections.emptyList();
    private final ServerRegistry serverRegistry;
    private final HealthCheck healthCheck;
    private final AtomicInteger position = new AtomicInteger(0);

    public RoundRobin(ServerRegistry serverRegistry, HealthCheck healthCheck) {
        this.serverRegistry = serverRegistry;
        this.healthCheck = healthCheck;
    }

    public void init() {
        populateServers();
        startHealthCheckBasedPruning();
    }

    public void populateServers() {
        this.servers = Collections.unmodifiableList(serverRegistry.getServers());
    }

    @Override
    public String getServer(String clientKey) {
        List<String> currentServers = servers;
        if (currentServers == null || currentServers.isEmpty()) {
            return null;
        }
        
        int current = position.getAndIncrement();
        int index = current % currentServers.size();
        if (index < 0) {
            index += currentServers.size();
        }
        
        return currentServers.get(index);
    }

    private void doHealthCheck() {
        List<String> healthyServers = healthCheck.getHealthyServers();
        this.servers = Collections.unmodifiableList(healthyServers);
    }

    public void startHealthCheckBasedPruning() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    doHealthCheck();
                    Thread.sleep(10_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }
}
