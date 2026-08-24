package com.example.loadbalancer.service;

import com.example.loadbalancer.service.schedulingalgorithms.SchedulingAlgorithm;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Enumeration;

@Component
public class LoadBalancerService {

    private final SchedulingAlgorithm schedulingAlgorithm;
    private final HttpClient client = HttpClient.newHttpClient();

    public LoadBalancerService( SchedulingAlgorithm scheduligAlgorithm ){
        this.schedulingAlgorithm = scheduligAlgorithm;
    }

    public ResponseEntity<byte[]> forward(HttpServletRequest request) throws IOException, InterruptedException {

        String clientKey;

        String authorization = request.getHeader("Authorization");

        if(authorization!=null && authorization.startsWith("Bearer ")){
            String jwtToken = authorization.substring(7);
            clientKey = jwtToken;
        }
        else{
            clientKey = request.getRemoteAddr();
        }

        String server = schedulingAlgorithm.getServer(clientKey);

        String targetUrl = server + request.getRequestURI();

        if( request.getQueryString()!=null ){
            targetUrl += "?" + request.getQueryString();
        }

        byte[] body = request.getInputStream().readAllBytes();

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .method(
                        request.getMethod(),
                        body.length > 0 ?
                                HttpRequest.BodyPublishers.ofByteArray(body)
                                : HttpRequest.BodyPublishers.noBody()
                );

        Enumeration<String> headerNames = request.getHeaderNames();

        while( headerNames.hasMoreElements() ){
            String headerName = headerNames.nextElement();
            if( headerName.equalsIgnoreCase("Host")
                    || headerName.equalsIgnoreCase("Connection") ) continue;

            Enumeration<String> values = request.getHeaders(headerName);

            while(values.hasMoreElements()){
                builder.header(
                        headerName, values.nextElement()
                );
            }
        }

        HttpResponse<byte[]> response = client.send(
                builder.build(),
                HttpResponse.BodyHandlers.ofByteArray()
        );

        HttpHeaders responseHeaders = new HttpHeaders();

        response.headers().map().forEach(
                (name, values) -> responseHeaders.put(name, values)
        );

        return ResponseEntity
                .status(response.statusCode())
                .headers(responseHeaders)
                .body(response.body());
    }

}
