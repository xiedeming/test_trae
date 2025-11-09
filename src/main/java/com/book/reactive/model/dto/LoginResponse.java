package com.book.reactive.model.dto;

public class LoginResponse {
    private String token;
    private String userName;
    private Long userId;
    private String userType;
    private Long userRole;

    public LoginResponse(String token, String userName, Long userId, String userType, Long userRole) {
        this.token = token;
        this.userName = userName;
        this.userId = userId;
        this.userType = userType;
        this.userRole = userRole;
    }

    // Getters and Setters
    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public Long getUserRole() {
        return userRole;
    }

    public void setUserRole(Long userRole) {
        this.userRole = userRole;
    }
}