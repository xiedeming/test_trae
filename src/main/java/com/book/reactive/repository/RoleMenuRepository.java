package com.book.reactive.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.RoleMenu;

public interface RoleMenuRepository extends ReactiveCrudRepository<RoleMenu, Long> {
}