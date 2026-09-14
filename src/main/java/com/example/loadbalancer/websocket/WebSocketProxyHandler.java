package com.example.loadbalancer.websocket;

import com.example.loadbalancer.service.schedulingalgorithms.SchedulingAlgorithm;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.net.URI;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebSocket reverse proxy handler using Spring's ReactorNettyWebSocketClient.
 *
 * For each frontend WebSocket connection, opens a corresponding backend
 * WebSocket connection and bridges messages in both directions using
 * reactive streams.
 */
@Component
public class WebSocketProxyHandler extends TextWebSocketHandler {

    private final SchedulingAlgorithm schedulingAlgorithm;
    private final ReactorNettyWebSocketClient reactorClient = new ReactorNettyWebSocketClient();
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    private static final String SINK_KEY = "messageSink";
    private static final String DISPOSABLE_KEY = "backendDisposable";

    public WebSocketProxyHandler(SchedulingAlgorithm schedulingAlgorithm, MeterRegistry meterRegistry) {
        this.schedulingAlgorithm = schedulingAlgorithm;
        meterRegistry.gauge("proxy.websocket.active_connections", activeConnections);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession clientSession) {
        activeConnections.incrementAndGet();

        // Extract JWT from query string to use as consistent hashing key,
        // matching the HTTP proxy which also hashes by JWT token.
        // This ensures a user's HTTP and WebSocket traffic land on the same backend.
        String clientKey = extractTokenFromQuery(clientSession.getUri().getQuery());
        if (clientKey == null) {
            clientKey = clientSession.getRemoteAddress() != null
                ? clientSession.getRemoteAddress().getAddress().getHostAddress()
                : "unknown";
        }

        String server = schedulingAlgorithm.getServer(clientKey);

        // Build backend URL, preserving query string (contains JWT token)
        String backendUrl = server.replaceFirst("^http", "ws") + clientSession.getUri().getPath();
        String query = clientSession.getUri().getQuery();
        if (query != null && !query.isEmpty()) {
            backendUrl += "?" + query;
        }

        URI backendUri = URI.create(backendUrl);

        // Create a Sink that the frontend→backend direction writes to.
        // The reactive WebSocket session will subscribe to this sink and
        // send messages to the backend as they arrive.
        Sinks.Many<String> outboundSink = Sinks.many().unicast().onBackpressureBuffer();
        clientSession.getAttributes().put(SINK_KEY, outboundSink);

        // Connect to the backend WebSocket
        Disposable disposable = reactorClient.execute(backendUri, backendSession -> {
            // Backend → Frontend: forward all messages from backend to the client
            Disposable inbound = backendSession.receive()
                    .map(WebSocketMessage::getPayloadAsText)
                    .subscribe(
                            text -> {
                                try {
                                    if (clientSession.isOpen()) {
                                        clientSession.sendMessage(new TextMessage(text));
                                    }
                                } catch (Exception e) {
                                    System.err.println("[WS-PROXY] Error forwarding backend→frontend: " + e.getMessage());
                                }
                            },
                            error -> {
                                System.err.println("[WS-PROXY] Backend receive error: " + error.getMessage());
                                closeQuietly(clientSession);
                            }
                    );

            // Frontend → Backend: send messages from the sink to the backend
            Flux<WebSocketMessage> outbound = outboundSink.asFlux()
                    .map(text -> backendSession.textMessage(text));

            return backendSession.send(outbound)
                    .doFinally(signal -> {
                        inbound.dispose();
                        closeQuietly(clientSession);
                    });
        }).subscribe(
                null,
                error -> {
                    System.err.println("[WS-PROXY] Backend connection failed: " + error.getMessage());
                    closeQuietly(clientSession);
                }
        );

        clientSession.getAttributes().put(DISPOSABLE_KEY, disposable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleTextMessage(WebSocketSession clientSession, TextMessage message) {
        Sinks.Many<String> sink = (Sinks.Many<String>) clientSession.getAttributes().get(SINK_KEY);
        if (sink != null) {
            sink.tryEmitNext(message.getPayload());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterConnectionClosed(WebSocketSession clientSession, CloseStatus status) {
        activeConnections.decrementAndGet();

        // Complete the sink so the backend send() completes
        Sinks.Many<String> sink = (Sinks.Many<String>) clientSession.getAttributes().get(SINK_KEY);
        if (sink != null) {
            sink.tryEmitComplete();
        }

        // Dispose the backend connection
        Disposable disposable = (Disposable) clientSession.getAttributes().get(DISPOSABLE_KEY);
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
        }
    }

    /**
     * Extract the JWT token from a query string like "token=eyJhbG..."
     * Returns null if no token parameter is found.
     */
    private String extractTokenFromQuery(String query) {
        if (query == null || query.isEmpty()) return null;
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2 && "token".equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
    }

    private void closeQuietly(WebSocketSession session) {
        try {
            if (session.isOpen()) {
                session.close(CloseStatus.SERVER_ERROR);
            }
        } catch (Exception ignored) {
        }
    }
}
