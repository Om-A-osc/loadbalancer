package com.example.loadbalancer.health;
import java.util.List;
import java.util.Set;

public interface HealthCheck {
    public List<String> getHealthyServers();
}
