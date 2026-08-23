package com.example.loadbalancer.health;
import com.example.loadbalancer.service.registry.ServerRegistry;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class QuorumGossipProtocol implements HealthCheck{

    private final ServerRegistry serverRegistry;
    private final HttpClient client = HttpClient.newHttpClient();

    public QuorumGossipProtocol( ServerRegistry serverRegistry ){
        this.serverRegistry = serverRegistry;
    }

    @Override
    public List<String> getHealthyServers(){
        List<String> healthyServers = new ArrayList<>();

        List< CompletableFuture< HttpResponse<String> > > futures = new ArrayList<>();

        for( String server : serverRegistry.getServers() ){
            String baseURI = server + "/health";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseURI))
                    .GET()
                    .build();
            futures.add(client.sendAsync(request, HttpResponse.BodyHandlers.ofString()));
        }

        try {
            CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
            ).join();
        } catch (Exception ignored) {
        }

        for( int i = 0 ; i < futures.size(); i++ ){
            try{
                HttpResponse response = futures.get(i).join();
                if( response.statusCode()==200 ){
                    healthyServers.add(serverRegistry.getServers().get(i));
                }
            }
            catch( Exception e ){

            }
        }

        return healthyServers;
    }

}
