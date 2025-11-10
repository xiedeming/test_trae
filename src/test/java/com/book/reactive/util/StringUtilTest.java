package com.book.reactive.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * StringUtil工具类测试
 */
public class StringUtilTest {

    private final StringUtil stringUtil = new StringUtil();
    
    @Test
    public void testExtractFeatureCode_NullInput() {
        String result = stringUtil.extractFeatureCode(null);
        assertEquals("", result, "null输入应该返回空字符串");
    }
    
    @Test
    public void testExtractFeatureCode_EmptyInput() {
        String result = stringUtil.extractFeatureCode("");
        assertEquals("", result, "空字符串输入应该返回空字符串");
    }
    
    @Test
    public void testExtractFeatureCode_ShortString() {
        String shortStr = "HelloWorld";
        String result = stringUtil.extractFeatureCode(shortStr);
        assertEquals(shortStr, result, "短字符串应该原样返回");
    }
    
    @Test
    public void testExtractFeatureCode_Exactly20Chars() {
        String exact20Str = "01234567890123456789";
        String result = stringUtil.extractFeatureCode(exact20Str);
        assertEquals(exact20Str, result, "恰好20位的字符串应该原样返回");
    }
    
    @Test
    public void testExtractFeatureCode_LongString() {
        String longStr = "这是一个很长的字符串，用于测试特征码提取功能，确保能够正确提取起始和末尾的字符。";
        String result = stringUtil.extractFeatureCode(longStr);
        
        // 验证结果格式
        assertTrue(result.contains("..."), "结果应该包含分隔符");
        
        // 验证前缀
        String expectedPrefix = longStr.substring(0, 10);
        assertTrue(result.startsWith(expectedPrefix), "结果应该以正确的前缀开头");
        
        // 验证后缀
        String expectedSuffix = longStr.substring(longStr.length() - 10);
        assertTrue(result.endsWith(expectedSuffix), "结果应该以正确的后缀结尾");
    }
    
    @Test
    public void testExtractFeatureCode_CustomLength() {
        String longStr = "这是一个很长的字符串，用于测试自定义长度的特征码提取。";
        int prefixLength = 5;
        int suffixLength = 8;
        
        String result = stringUtil.extractFeatureCode(longStr, prefixLength, suffixLength);
        
        // 验证前缀长度
        String expectedPrefix = longStr.substring(0, prefixLength);
        assertTrue(result.startsWith(expectedPrefix), "前缀长度应该正确");
        
        // 验证后缀长度
        String expectedSuffix = longStr.substring(longStr.length() - suffixLength);
        assertTrue(result.endsWith(expectedSuffix), "后缀长度应该正确");
        
        // 验证分隔符
        assertTrue(result.contains("..."), "结果应该包含分隔符");
    }
    
    @Test
    public void testExtractFeatureCode_NegativeLengths() {
        String input = "测试字符串";
        // 负长度应该被修正为0
        String result = stringUtil.extractFeatureCode(input, -5, -3);
        assertEquals(input, result, "负长度参数应该被修正为0");
    }
    
    @Test
    public void testExtractFeatureCode_ZeroLengths() {
        String longStr = "01234567890123456789ABCDEF";
        // 零长度前缀和后缀
        String result = stringUtil.extractFeatureCode(longStr, 0, 0);
        assertEquals(longStr, result, "零长度参数应该返回原字符串");
    }
}