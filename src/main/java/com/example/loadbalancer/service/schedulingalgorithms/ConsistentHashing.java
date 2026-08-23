package com.example.loadbalancer.service.schedulingalgorithms;

import com.example.loadbalancer.health.HealthCheck;
import com.example.loadbalancer.service.registry.ServerRegistry;
import jakarta.annotation.PostConstruct;
import org.apache.commons.codec.digest.MurmurHash3;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;


@Component
public class ConsistentHashing implements SchedulingAlgorithm {


    private volatile NavigableMap<Long, String> serverSortedMap = Collections.emptyNavigableMap();
    private final ServerRegistry serverRegistry;
    private final HealthCheck healthCheck;
    private final int virtualNodeCount;

    public NavigableMap<Long, String> getServerSortedMap() {
        return serverSortedMap;
    }

    public void setServerSortedMap(NavigableMap<Long, String> serverSortedMap) {
        this.serverSortedMap = serverSortedMap;
    }

    public ConsistentHashing(ServerRegistry serverRegistry, HealthCheck healthCheck, @Value("${spring.virtual-node-count}") int virtualNodeCount){
        this.serverRegistry = serverRegistry;
        this.healthCheck = healthCheck;
        this.virtualNodeCount = virtualNodeCount;
    }

    @PostConstruct
    public void populateServerSortedMap(){
        replaceCurrentServerMap(serverRegistry.getServers(), virtualNodeCount);
    }

    private void replaceCurrentServerMap(List<String> healthyServers, int virtualNodeCount ){
        TreeMap<Long, String> treeMap = new TreeMap<>();
        for( String server : healthyServers ){
            for( int i = 0 ; i < virtualNodeCount ; i++ ){
                Long hashedServerKey = hash(server+"#"+i);
                treeMap.put(hashedServerKey,server);
            }
        }
        setServerSortedMap(Collections.unmodifiableNavigableMap(treeMap));
    }

    public Long hash(String key){
        return Integer.toUnsignedLong(MurmurHash3.hash32(key));
    }

    @Override
    public String getServer( String clientKey ){
            Long clientKeyHash = hash(clientKey);
            Long clockwiseClosestServer = serverSortedMap.higherKey(clientKeyHash);
            if( clockwiseClosestServer==null ){
                return serverSortedMap.firstEntry().getValue();
            }
            return serverSortedMap.get(clockwiseClosestServer);
    }

    private void doHealthCheck(){
        List<String> healthyServer = healthCheck.getHealthyServers();
        replaceCurrentServerMap(healthyServer, virtualNodeCount);
    }

    @PostConstruct
    public void startHealthCheckBasedPruning(){
        Thread thread = new Thread(()-> {
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
