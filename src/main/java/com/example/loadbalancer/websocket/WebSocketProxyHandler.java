package com.example.loadbalancer.websocket;

import com.example.loadbalancer.service.schedulingalgorithms.SchedulingAlgorithm;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;

@Component
public class WebSocketProxyHandler extends TextWebSocketHandler {

    private final SchedulingAlgorithm schedulingAlgorithm;
    private final HttpClient client = HttpClient.newHttpClient();

    public WebSocketProxyHandler(SchedulingAlgorithm schedulingAlgorithm){
        this.schedulingAlgorithm = schedulingAlgorithm;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession clientSession){
        String clientKey = clientSession.getRemoteAddress().getAddress().getHostAddress();

        String server = schedulingAlgorithm.getServer(clientKey);

        String backendUrl = server.replaceFirst("^http","ws") + clientSession.getUri().getPath();

        URI backendUri = URI.create(backendUrl);

        BackendWebSocketListener listener = new BackendWebSocketListener(clientSession);

        CompletableFuture<WebSocket> future = client.newWebSocketBuilder().buildAsync(backendUri,listener);

        future.whenComplete((backendSocket, error)->{
            if( error!=null ){
                try{
                    clientSession.close(
                            CloseStatus.SERVER_ERROR
                    );
                } catch ( Exception ignored ){

                }
                return;
            }
            clientSession.getAttributes().put("backendSocket", backendSocket);
        });
    }

    @Override
    public void handleTextMessage(WebSocketSession clientSession, TextMessage message){
        WebSocket backendSocket = (WebSocket) clientSession.getAttributes().get("backendSocket");
        if(backendSocket!=null){
            backendSocket.sendText(message.getPayload(),true);
        }
    }

    @Override
    public void afterConnectionClosed(
            WebSocketSession clientSession,
            CloseStatus status
    ){
        WebSocket backendSocket = (WebSocket) clientSession.getAttributes().get("backendSocket");

        if( backendSocket!=null ){
            backendSocket.sendClose(WebSocket.NORMAL_CLOSURE,"Client disconnected");
        }
    }

}
