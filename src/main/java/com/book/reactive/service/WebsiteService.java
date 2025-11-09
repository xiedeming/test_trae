package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.book.reactive.model.Website;
import com.book.reactive.repository.WebsiteRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class WebsiteService {

    @Autowired
    private WebsiteRepository websiteRepository;

    public Flux<Website> findAll() {
        return websiteRepository.findAll();
    }

    public Mono<Website> findById(Long id) {
        return websiteRepository.findById(id);
    }

    public Mono<Website> save(Website website) {
        LocalDateTime now = LocalDateTime.now();
        website.setCreateTime(now);
        website.setModifyTime(now);
        return websiteRepository.save(website);
    }

    public Mono<Website> update(Long id, Website website) {
        return websiteRepository.findById(id)
                .flatMap(existingWebsite -> {
                    existingWebsite.setWebsiteName(website.getWebsiteName());
                    existingWebsite.setWebsiteUrl(website.getWebsiteUrl());
                    existingWebsite.setWebsiteSummary(website.getWebsiteSummary());
                    existingWebsite.setModifyUserId(website.getModifyUserId());
                    existingWebsite.setModifyUserName(website.getModifyUserName());
                    existingWebsite.setModifyTime(LocalDateTime.now());
                    return websiteRepository.save(existingWebsite);
                });
    }

    public Mono<Void> delete(Long id) {
        return websiteRepository.deleteById(id);
    }
}