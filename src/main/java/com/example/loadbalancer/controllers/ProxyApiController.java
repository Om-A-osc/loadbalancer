package com.example.loadbalancer.controllers;

import com.example.loadbalancer.service.LoadBalancerService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class ProxyApiController {
    private final LoadBalancerService loadBalancerService;
    public ProxyApiController( LoadBalancerService loadbalancerService){
        this.loadBalancerService = loadbalancerService;
    }


    @RequestMapping("/**")
    public ResponseEntity<?> proxyRequest(HttpServletRequest request) throws IOException, InterruptedException {
        return loadBalancerService.forward(request);
    }

}
