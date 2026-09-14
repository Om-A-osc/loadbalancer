package com.example.loadbalancer.config;

import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

/**
 * Shared WebClient for proxying and feed aggregation.
 *
 * The default Reactor Netty pool allows only 2 x cores connections per backend
 * with a 45 second acquire wait and a 256 KB response limit, none of which
 * survive 1000+ concurrent proxied requests or a multi-megabyte feed.
 */
@Configuration
public class WebClientConfig {

    @Bean
    public WebClient proxyWebClient(
            @Value("${lb.max-connections:3000}") int maxConnections,
            @Value("${lb.connect-timeout-ms:1000}") int connectTimeoutMs,
            @Value("${lb.response-timeout-ms:10000}") long responseTimeoutMs,
            @Value("${lb.max-body-bytes:67108864}") int maxBodyBytes) {

        ConnectionProvider provider = ConnectionProvider.builder("lb-proxy")
                .maxConnections(maxConnections)
                .pendingAcquireMaxCount(-1)
                .pendingAcquireTimeout(Duration.ofMillis(responseTimeoutMs))
                .build();

        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .responseTimeout(Duration.ofMillis(responseTimeoutMs));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(maxBodyBytes))
                .build();
    }
}
