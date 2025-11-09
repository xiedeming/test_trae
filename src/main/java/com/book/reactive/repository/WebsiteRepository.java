package com.book.reactive.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.Website;

public interface WebsiteRepository extends ReactiveCrudRepository<Website, Long> {
}