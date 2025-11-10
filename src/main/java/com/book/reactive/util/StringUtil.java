package com.book.reactive.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.springframework.stereotype.Component;

/**
 * 字符串处理工具类
 */
@Component
public class StringUtil {

    /**
     * 提取长字符串的特征码（起始10位和末尾10位）
     * @param input 输入的长字符串
     * @return 特征码，如果输入为空则返回空字符串
     */
    public String extractFeatureCode(String input) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        
        // 如果字符串长度小于等于20位，则直接返回原字符串
        if (input.length() <= 20) {
            return input;
        }
        
        // 提取起始10位
        String startPart = input.substring(0, 10);
        // 提取末尾10位
        String endPart = input.substring(input.length() - 10);
        
        // 组合特征码，使用...分隔
        return md5(startPart+"..." + endPart);
    }

    public static String md5(String input) {
        try {
            // 获取 MD5 消息摘要实例
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 计算 MD5 哈希值
            byte[] hashBytes = md.digest(input.getBytes(StandardCharsets.UTF_8));

            // 将字节数组转换为 16 进制字符串
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString(); // 返回 32 位 16 进制 MD5 字符串
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 算法不可用", e);
        }
    }
    
    /**
     * 提取长字符串的特征码（可自定义长度）
     * @param input 输入的长字符串
     * @param prefixLength 前缀长度
     * @param suffixLength 后缀长度
     * @return 特征码
     */
    public String extractFeatureCode(String input, int prefixLength, int suffixLength) {
        if (input == null || input.isEmpty()) {
            return "";
        }
        
        // 验证长度参数
        prefixLength = Math.max(0, prefixLength);
        suffixLength = Math.max(0, suffixLength);
        
        // 如果字符串长度小于等于总提取长度，则直接返回原字符串
        if (input.length() <= prefixLength + suffixLength) {
            return input;
        }
        
        // 提取前缀和后缀
        String startPart = input.substring(0, prefixLength);
        String endPart = input.substring(input.length() - suffixLength);
        
        // 组合特征码
        return startPart + "..." + endPart;
    }
}