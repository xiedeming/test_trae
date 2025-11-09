package com.book.reactive.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.book.reactive.model.User;
import com.book.reactive.model.dto.LoginRequest;
import com.book.reactive.model.dto.LoginResponse;
import com.book.reactive.repository.UserRepository;
import com.book.reactive.util.JwtUtil;
import com.book.reactive.util.PasswordUtil;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private JwtUtil jwtUtil;
    
    @Autowired
    private PasswordUtil passwordUtil;

    public Flux<User> findAll() {
        return userRepository.findAll();
    }

    public Mono<User> findById(Long id) {
        return userRepository.findById(id);
    }

    public Mono<User> save(User user, Long currentUserId, String currentUserName) {
        // 设置创建用户信息
        user.setCreateUserId(currentUserId);
        user.setCreateUserName(currentUserName);
        
        // 如果是新用户且有密码，需要加密
        if (user.getUserPwd() != null && !user.getUserPwd().isEmpty() && user.getUserSalt() == null) {
            String salt = passwordUtil.generateSalt();
            user.setUserSalt(salt);
            user.setUserPwd(passwordUtil.encryptPassword(user.getUserPwd(), salt));
        }
        
        return userRepository.save(user);
    }
    
    // 兼容旧方法，默认使用系统用户
    public Mono<User> save(User user) {
        return save(user, 0L, "system");
    }

    public Mono<User> update(Long id, User user, Long currentUserId, String currentUserName) {
        return userRepository.findById(id)
                .flatMap(existingUser -> {
                    // 更新用户信息
                    existingUser.setUserName(user.getUserName());
                    existingUser.setUserType(user.getUserType());
                    existingUser.setUserRole(user.getUserRole());
                    existingUser.setUserEmail(user.getUserEmail());
                    existingUser.setUserPhone(user.getUserPhone());
                    existingUser.setUserSummary(user.getUserSummary());
                    
                    // 更新修改人信息
                    existingUser.setModifyUserId(currentUserId);
                    existingUser.setModifyUserName(currentUserName);
                    
                    // 密码如果有更新，需要加密
                    if (user.getUserPwd() != null && !user.getUserPwd().isEmpty()) {
                        String salt = passwordUtil.generateSalt();
                        existingUser.setUserSalt(salt);
                        existingUser.setUserPwd(passwordUtil.encryptPassword(user.getUserPwd(), salt));
                    }
                    
                    return userRepository.save(existingUser);
                });
    }
    
    // 兼容旧方法，默认使用系统用户
    public Mono<User> update(Long id, User user) {
        return update(id, user, 0L, "system");
    }

    public Mono<Void> delete(Long id) {
        return userRepository.deleteById(id);
    }

    public Flux<User> findByUserNameContaining(String userName) {
        return userRepository.findByUserNameContaining(userName);
    }
    
    public Mono<User> findByUserEmail(String userEmail) {
        return userRepository.findByUserEmail(userEmail);
    }
    
    public Flux<User> findByUserType(String userType) {
        return userRepository.findByUserType(userType);
    }
    
    public Flux<User> findByUserRole(Long userRole) {
        return userRepository.findByUserRole(userRole);
    }
    
    public Mono<User> findByUserPhone(String userPhone) {
        return userRepository.findByUserPhone(userPhone);
    }
    
    // 登录方法
    public Mono<LoginResponse> login(LoginRequest loginRequest) {
        return userRepository.findByUserName(loginRequest.getUserName())
                .switchIfEmpty(Mono.error(new RuntimeException("用户名或密码错误")))
                .flatMap(user -> {
                    // 验证密码
                    if (passwordUtil.verifyPassword(loginRequest.getUserPwd(), user.getUserPwd(), user.getUserSalt())) {
                        // 生成token（异步）
                        return jwtUtil.generateToken(user.getId(), user.getUserName())
                                .flatMap(token -> {
                                    // 返回登录响应
                                    return Mono.just(new LoginResponse(
                                            token,
                                            user.getUserName(),
                                            user.getId(),
                                            user.getUserType(),
                                            user.getUserRole()
                                    ));
                                });
                    } else {
                        return Mono.error(new RuntimeException("用户名或密码错误"));
                    }
                });
    }
}