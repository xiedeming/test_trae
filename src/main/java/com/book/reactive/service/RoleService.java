package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.book.reactive.model.Role;
import com.book.reactive.repository.RoleRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class RoleService {

    @Autowired
    private RoleRepository roleRepository;

    public Flux<Role> findAll() {
        return roleRepository.findAll();
    }

    public Mono<Role> findById(Long id) {
        return roleRepository.findById(id);
    }

    public Mono<Role> save(Role role) {
        LocalDateTime now = LocalDateTime.now();
        role.setCreateTime(now);
        role.setModifyTime(now);
        return roleRepository.save(role);
    }

    public Mono<Role> update(Long id, Role role) {
        return roleRepository.findById(id)
                .flatMap(existingRole -> {
                    existingRole.setRoleName(role.getRoleName());
                    existingRole.setRoleType(role.getRoleType());
                    existingRole.setRoleSummary(role.getRoleSummary());
                    existingRole.setModifyUserId(role.getModifyUserId());
                    existingRole.setModifyUserName(role.getModifyUserName());
                    existingRole.setModifyTime(LocalDateTime.now());
                    return roleRepository.save(existingRole);
                });
    }

    public Mono<Void> delete(Long id) {
        return roleRepository.deleteById(id);
    }
}