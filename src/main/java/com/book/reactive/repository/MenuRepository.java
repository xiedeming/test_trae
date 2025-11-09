package com.book.reactive.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.Menu;

public interface MenuRepository extends ReactiveCrudRepository<Menu, Long> {
}