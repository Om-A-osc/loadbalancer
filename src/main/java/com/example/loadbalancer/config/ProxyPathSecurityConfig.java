package com.example.loadbalancer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;

/**
 * Keeps Spring Security's filter chain off the hot proxied paths. The chain
 * permits everything anyway, so on /message it only costs CPU, which is the
 * scarce resource on a 1 CPU balancer. The existing SecurityConfig and its
 * CORS handling still apply to every other route.
 */
@Configuration
public class ProxyPathSecurityConfig {

    @Bean
    public WebSecurityCustomizer proxyPathsIgnored() {
        return web -> web.ignoring().requestMatchers("/message", "/feed", "/feed/**", "/lb/**");
    }
}
