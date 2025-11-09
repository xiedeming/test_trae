package com.book.reactive.util;

import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

@Component
public class PasswordUtil {

    private static final String SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int SALT_LENGTH = 8;

    // 生成随机盐值
    public String generateSalt() {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(SALT_LENGTH);
        for (int i = 0; i < SALT_LENGTH; i++) {
            sb.append(SALT_CHARS.charAt(random.nextInt(SALT_CHARS.length())));
        }
        return sb.toString();
    }

    // 加密密码
    public String encryptPassword(String password, String salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String saltedPassword = password + salt;
            byte[] hashBytes = md.digest(saltedPassword.getBytes());
            return bytesToHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error encrypting password", e);
        }
    }

    // 验证密码
    public boolean verifyPassword(String rawPassword, String encryptedPassword, String salt) {
        String hashedInput = encryptPassword(rawPassword, salt);
        return hashedInput.equals(encryptedPassword);
    }

    // 将字节数组转换为十六进制字符串
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}