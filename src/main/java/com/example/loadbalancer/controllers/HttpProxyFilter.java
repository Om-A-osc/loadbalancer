package com.example.loadbalancer.filters;

import com.example.loadbalancer.service.LoadBalancerService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class HttpProxyFilter extends OncePerRequestFilter {

    private final LoadBalancerService loadBalancerService;

    public HttpProxyFilter(LoadBalancerService loadBalancerService) {
        this.loadBalancerService = loadBalancerService;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain)
            throws ServletException, IOException {

        // Let WebSocket upgrade requests continue to
        // Spring's WebSocket infrastructure.
        String upgrade = request.getHeader("Upgrade");

        if (upgrade != null &&
                upgrade.equalsIgnoreCase("websocket")) {

            filterChain.doFilter(request, response);
            return;
        }

        try {
            ResponseEntity<byte[]> backendResponse =
                    loadBalancerService.forward(request);

            response.setStatus(
                    backendResponse.getStatusCode().value()
            );

            backendResponse.getHeaders().forEach(
                    (name, values) -> {
                        for (String value : values) {
                            response.addHeader(name, value);
                        }
                    }
            );

            byte[] body = backendResponse.getBody();

            if (body != null) {
                response.getOutputStream().write(body);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServletException(e);
        }
    }
}