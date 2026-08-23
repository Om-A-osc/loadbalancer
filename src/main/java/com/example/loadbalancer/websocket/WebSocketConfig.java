package com.example.loadbalancer.websocket;


import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    private final WebSocketProxyHandler handler;
    public WebSocketConfig(WebSocketProxyHandler handler){
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers( WebSocketHandlerRegistry registry ){
        registry.addHandler(handler, "/ws")
                .setAllowedOrigins("*");
    }
}
