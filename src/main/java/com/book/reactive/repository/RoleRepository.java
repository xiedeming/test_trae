package com.book.reactive.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.book.reactive.model.Role;

public interface RoleRepository extends ReactiveCrudRepository<Role, Long> {
}