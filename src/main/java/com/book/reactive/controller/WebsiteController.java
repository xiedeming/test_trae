package com.book.reactive.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.book.reactive.model.Website;
import com.book.reactive.service.WebsiteService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/websites")
public class WebsiteController {

    @Autowired
    private WebsiteService websiteService;

    @GetMapping
    public Flux<Website> getAllWebsites() {
        return websiteService.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Website>> getWebsiteById(@PathVariable Long id) {
        return websiteService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Website> createWebsite(@RequestBody Website website) {
        return websiteService.save(website);
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<Website>> updateWebsite(@PathVariable Long id, @RequestBody Website website) {
        return websiteService.update(id, website)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<ResponseEntity<Void>> deleteWebsite(@PathVariable Long id) {
        return websiteService.delete(id)
                .then(Mono.just(new ResponseEntity<Void>(HttpStatus.NO_CONTENT)));
    }
}