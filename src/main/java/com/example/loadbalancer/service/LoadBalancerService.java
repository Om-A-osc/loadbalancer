package com.example.loadbalancer.service;

import com.example.loadbalancer.service.schedulingalgorithms.SchedulingAlgorithm;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Set;

/**
 * Production-grade HTTP proxy using Spring WebFlux's WebClient (Netty-based).
 * Unlike JDK's HttpClient, WebClient/Netty has NO restricted header limitations.
 */
@Component
public class LoadBalancerService {

    private final SchedulingAlgorithm schedulingAlgorithm;
    private final WebClient webClient;

    /**
     * Hop-by-hop headers that should not be forwarded between proxy hops
     * (per HTTP/1.1 RFC 2616 §13.5.1 and RFC 7230 §6.1).
     */
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            "host",
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "proxy-connection",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
            "access-control-allow-origin",
            "access-control-allow-methods",
            "access-control-allow-headers",
            "access-control-allow-credentials",
            "access-control-max-age",
            "access-control-expose-headers"
    );

    private final BackendLoadTracker loadTracker;

    public LoadBalancerService(SchedulingAlgorithm schedulingAlgorithm,
                               WebClient proxyWebClient,
                               BackendLoadTracker loadTracker) {
        this.schedulingAlgorithm = schedulingAlgorithm;
        // Shared, properly sized client from WebClientConfig (large pool + timeouts)
        this.webClient = proxyWebClient;
        this.loadTracker = loadTracker;
    }

    /** Blocking variant, kept for callers that need a synchronous result. */
    public ResponseEntity<byte[]> forward(HttpServletRequest request) throws IOException {
        return forwardAsync(request, request.getInputStream().readAllBytes()).block();
    }

    /**
     * Non-blocking forward. Everything that touches the servlet request is
     * evaluated before this method returns; the returned Mono completes on a
     * Netty thread with the backend's response (or a 502/503 entity), so the
     * caller can release its Tomcat thread while the backend works.
     */
    public Mono<ResponseEntity<byte[]>> forwardAsync(HttpServletRequest request, byte[] body) {

        String clientKey;
        String authorization = request.getHeader("Authorization");

        if (authorization != null && authorization.startsWith("Bearer ")) {
            clientKey = authorization.substring(7);
        } else {
            String forwardedFor = request.getHeader("X-Forwarded-For");
            if (forwardedFor != null && !forwardedFor.isEmpty()) {
                clientKey = forwardedFor.split(",")[0].trim();
            } else {
                clientKey = java.util.UUID.randomUUID().toString();
            }
        }

        String server = schedulingAlgorithm.getServer(clientKey);
        if (server == null) {
            return Mono.just(ResponseEntity.status(503).body("No backend available".getBytes()));
        }
        String targetUrl = server + request.getRequestURI();

        if (request.getQueryString() != null) {
            targetUrl += "?" + request.getQueryString();
        }

        HttpMethod method = HttpMethod.valueOf(request.getMethod());

        // Build the proxy request. Pass the target as a java.net.URI so an
        // already-encoded query string (e.g. %20) is forwarded verbatim instead
        // of being encoded a second time by the URI template expander.
        WebClient.RequestBodySpec requestSpec = webClient
                .method(method)
                .uri(toUri(targetUrl))
                .headers(headers -> copyRequestHeaders(request, headers));

        // Attach body if present
        WebClient.RequestHeadersSpec<?> finalSpec;
        if (body.length > 0) {
            // Preserve the original Content-Type
            String contentType = request.getContentType();
            if (contentType != null) {
                finalSpec = requestSpec
                        .header(HttpHeaders.CONTENT_TYPE, contentType)
                        .bodyValue(body);
            } else {
                finalSpec = requestSpec.bodyValue(body);
            }
        } else {
            finalSpec = requestSpec;
        }

        // Execute without blocking, recording in-flight count and latency so
        // the scheduler can see each backend's live load.
        // exchangeToMono never throws on 4xx/5xx, so backend status codes pass
        // through unchanged. retrieve().onStatus(status -> false, ..) did not do
        // that: Spring keeps its default error handler last, so every backend
        // 4xx/5xx surfaced here as an exception and became a 502.
        loadTracker.onStart(server);
        long startedAt = System.nanoTime();
        return finalSpec
                .exchangeToMono(clientResponse -> clientResponse.toEntity(byte[].class))
                .map(response -> {
                    // Filter hop-by-hop headers from the response before returning
                    HttpHeaders cleanedHeaders = new HttpHeaders();
                    response.getHeaders().forEach((name, values) -> {
                        if (!HOP_BY_HOP_HEADERS.contains(name.toLowerCase())) {
                            cleanedHeaders.put(name, values);
                        }
                    });
                    return ResponseEntity
                            .status(response.getStatusCode())
                            .headers(cleanedHeaders)
                            .body(response.getBody());
                })
                // Connection refused or timeout: take this backend out of rotation briefly
                .doOnError(error -> loadTracker.markDown(server))
                .doFinally(signal -> loadTracker.onComplete(server,
                        (System.nanoTime() - startedAt) / 1_000_000.0,
                        signal == SignalType.ON_COMPLETE));
    }

    private static URI toUri(String url) {
        try {
            return URI.create(url);
        } catch (IllegalArgumentException notYetEncoded) {
            return UriComponentsBuilder.fromUriString(url).build().toUri();
        }
    }

    /**
     * Copy all headers from the incoming servlet request to the outgoing
     * WebClient request, skipping only hop-by-hop headers.
     */
    private void copyRequestHeaders(HttpServletRequest request, HttpHeaders headers) {
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();

            // Skip hop-by-hop headers (only truly protocol-level ones)
            if (HOP_BY_HOP_HEADERS.contains(headerName.toLowerCase())) continue;

            Enumeration<String> values = request.getHeaders(headerName);
            while (values.hasMoreElements()) {
                headers.add(headerName, values.nextElement());
            }
        }
    }
}
