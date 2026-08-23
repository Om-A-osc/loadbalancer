package com.example.loadbalancer.service.schedulingalgorithms;

public interface SchedulingAlgorithm {
    public String getServer( String clientKey );
}
