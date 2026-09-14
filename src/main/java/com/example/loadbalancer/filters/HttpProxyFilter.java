package com.example.loadbalancer.filters;

import com.example.loadbalancer.service.FeedVersion;
import com.example.loadbalancer.service.LoadBalancerService;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Filter that intercepts HTTP requests and forwards them to the backend
 * via WebClient, skipping WebSocket handshake requests.
 *
 * The forward is non-blocking: the Tomcat thread reads the request, starts
 * servlet async processing and hands off to Netty. The response is written
 * and the request completed from the Netty callback. No thread waits for
 * the backend, so a handful of Tomcat threads serves any number of
 * concurrent clients, which matters on a 1 CPU container where hundreds of
 * parked threads cost more in switching and memory than the requests do.
 */
@Component
public class HttpProxyFilter extends OncePerRequestFilter {

    private final LoadBalancerService loadBalancerService;
    private final FeedVersion feedVersion;
    private final com.example.loadbalancer.service.LbFeedStore replica;
    private final long asyncTimeoutMs;

    public HttpProxyFilter(LoadBalancerService loadBalancerService,
                           FeedVersion feedVersion,
                           com.example.loadbalancer.service.LbFeedStore replica,
                           @Value("${lb.async-timeout-ms:30000}") long asyncTimeoutMs) {
        this.loadBalancerService = loadBalancerService;
        this.feedVersion = feedVersion;
        this.replica = replica;
        this.asyncTimeoutMs = asyncTimeoutMs;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain)
            throws ServletException, IOException {

        // Skip WebSocket upgrades so they reach the WebSocketProxyHandler,
        // skip /actuator so the loadbalancer can serve its own metrics, and
        // skip /feed and /lb/* which the balancer answers itself (FeedAggregationController)
        String uri = request.getRequestURI();
        if (uri.startsWith("/ws") ||
            "websocket".equalsIgnoreCase(request.getHeader("Upgrade")) ||
            uri.startsWith("/actuator") ||
            uri.startsWith("/feed") ||
            uri.startsWith("/lb/")) {
            filterChain.doFilter(request, response);
            return;
        }

        // Everything that needs the servlet request is read here, on the Tomcat thread
        byte[] body = request.getInputStream().readAllBytes();
        String method = request.getMethod();
        boolean isMessage = uri.equals("/message");

        AsyncContext async = request.startAsync(request, response);
        async.setTimeout(asyncTimeoutMs);
        AtomicBoolean done = new AtomicBoolean(false);
        async.addListener(new AsyncListener() {
            @Override public void onTimeout(AsyncEvent event) throws IOException {
                if (done.compareAndSet(false, true)) {
                    writeError(response, 504, "Gateway Timeout");
                    async.complete();
                }
            }
            @Override public void onError(AsyncEvent event) { done.set(true); }
            @Override public void onComplete(AsyncEvent event) { }
            @Override public void onStartAsync(AsyncEvent event) { }
        });

        loadBalancerService.forwardAsync(request, body).subscribe(
                backendResponse -> {
                    if (!done.compareAndSet(false, true)) return;
                    try {
                        writeResponse(response, backendResponse);
                        // A completed /message changes the feed: invalidate the merged-feed cache
                        // A message that did not pass through the Netty front is not in the
                        // balancer's replica: merge from the shards until the next clear.
                        if (isMessage) { replica.markDirty(); feedVersion.bump(); }
                    } catch (IOException e) {
                        System.err.println("[PROXY] Error writing response for " + method + " " + uri + ": " + e.getMessage());
                    } finally {
                        async.complete();
                    }
                },
                error -> {
                    if (!done.compareAndSet(false, true)) return;
                    try {
                        // 502 written here, not by Tomcat's error page, so CORS headers survive
                        System.err.println("[PROXY] Error forwarding " + method + " " + uri + ": " + error.getMessage());
                        writeError(response, 502, "Bad Gateway: " + String.valueOf(error.getMessage()).replace("\"", "'"));
                    } catch (IOException e) {
                        System.err.println("[PROXY] Error writing 502 for " + method + " " + uri + ": " + e.getMessage());
                    } finally {
                        async.complete();
                    }
                });
    }

    private static void writeResponse(HttpServletResponse response, ResponseEntity<byte[]> backendResponse) throws IOException {
        response.setStatus(backendResponse.getStatusCode().value());
        backendResponse.getHeaders().forEach((name, values) -> {
            for (String value : values) {
                response.addHeader(name, value);
            }
        });
        byte[] body = backendResponse.getBody();
        if (body != null && body.length > 0) {
            response.getOutputStream().write(body);
        }
    }

    private static void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }
}
