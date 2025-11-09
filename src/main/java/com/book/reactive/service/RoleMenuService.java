package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.book.reactive.model.RoleMenu;
import com.book.reactive.repository.RoleMenuRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class RoleMenuService {

    @Autowired
    private RoleMenuRepository roleMenuRepository;

    public Flux<RoleMenu> findAll() {
        return roleMenuRepository.findAll();
    }

    public Mono<RoleMenu> findById(Long id) {
        return roleMenuRepository.findById(id);
    }

    public Mono<RoleMenu> save(RoleMenu roleMenu) {
        LocalDateTime now = LocalDateTime.now();
        roleMenu.setCreateTime(now);
        roleMenu.setModifyTime(now);
        return roleMenuRepository.save(roleMenu);
    }

    public Mono<RoleMenu> update(Long id, RoleMenu roleMenu) {
        return roleMenuRepository.findById(id)
                .flatMap(existingRoleMenu -> {
                    existingRoleMenu.setRoleId(roleMenu.getRoleId());
                    existingRoleMenu.setMenuId(roleMenu.getMenuId());
                    existingRoleMenu.setRoleMenuSummary(roleMenu.getRoleMenuSummary());
                    existingRoleMenu.setModifyUserId(roleMenu.getModifyUserId());
                    existingRoleMenu.setModifyUserName(roleMenu.getModifyUserName());
                    existingRoleMenu.setModifyTime(LocalDateTime.now());
                    return roleMenuRepository.save(existingRoleMenu);
                });
    }

    public Mono<Void> delete(Long id) {
        return roleMenuRepository.deleteById(id);
    }
}