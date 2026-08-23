package com.example.loadbalancer.service.registry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ServerRegistry {

    private final List<String> servers;

    public ServerRegistry(
            @Value("${spring.servers}") String servers
    ) {
        this.servers = Arrays.asList(servers.split(","));
    }

    public List<String> getServers() {
        return servers;
    }
}