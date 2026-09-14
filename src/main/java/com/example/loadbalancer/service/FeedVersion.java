package com.example.loadbalancer.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts /message requests that have completed through the proxy. The feed
 * cache is valid exactly while this number has not moved since it was built,
 * so a cached feed can never omit a message that was already accepted.
 */
@Component
public class FeedVersion {

    private final AtomicLong version = new AtomicLong();

    public void bump() {
        version.incrementAndGet();
    }

    public long get() {
        return version.get();
    }
}
