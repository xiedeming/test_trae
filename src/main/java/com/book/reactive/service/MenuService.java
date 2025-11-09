package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.book.reactive.model.Menu;
import com.book.reactive.repository.MenuRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class MenuService {

    @Autowired
    private MenuRepository menuRepository;

    public Flux<Menu> findAll() {
        return menuRepository.findAll();
    }

    public Mono<Menu> findById(Long id) {
        return menuRepository.findById(id);
    }

    public Mono<Menu> save(Menu menu) {
        LocalDateTime now = LocalDateTime.now();
        menu.setCreateTime(now);
        menu.setModifyTime(now);
        return menuRepository.save(menu);
    }

    public Mono<Menu> update(Long id, Menu menu) {
        return menuRepository.findById(id)
                .flatMap(existingMenu -> {
                    existingMenu.setMenuName(menu.getMenuName());
                    existingMenu.setRoleType(menu.getRoleType());
                    existingMenu.setMenuSummary(menu.getMenuSummary());
                    existingMenu.setModifyUserId(menu.getModifyUserId());
                    existingMenu.setModifyUserName(menu.getModifyUserName());
                    existingMenu.setModifyTime(LocalDateTime.now());
                    return menuRepository.save(existingMenu);
                });
    }

    public Mono<Void> delete(Long id) {
        return menuRepository.deleteById(id);
    }
}