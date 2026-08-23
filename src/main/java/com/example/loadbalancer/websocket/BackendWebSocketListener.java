package com.example.loadbalancer.websocket;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;

public class BackendWebSocketListener implements WebSocket.Listener{

    private final WebSocketSession clientSession;

    public BackendWebSocketListener(WebSocketSession clientSession){
        this.clientSession = clientSession;
    }

    @Override
    public void onOpen(WebSocket webSocket){
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket websocket, CharSequence data, boolean last){
        try {
            if (clientSession.isOpen()) {
                clientSession.sendMessage(
                        new TextMessage(data.toString())
                );
            }
        } catch (Exception e){
            try{
                clientSession.close(
                        CloseStatus.SERVER_ERROR
                );
            } catch (Exception ignnored){

            }
        }
        websocket.request(1);
        return null;
    }
    @Override
    public CompletionStage<?> onClose(
            WebSocket webSocket,
            int statusCode,
            String reason) {

        try {

            if (clientSession.isOpen()) {
                clientSession.close();
            }

        } catch (Exception ignored) {
        }

        return null;
    }

    @Override
    public void onError(
            WebSocket webSocket,
            Throwable error) {

        try {

            if (clientSession.isOpen()) {
                clientSession.close(
                        CloseStatus.SERVER_ERROR
                );
            }

        } catch (Exception ignored) {
        }
    }

}
