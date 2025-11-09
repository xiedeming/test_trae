package com.book.reactive.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import java.time.LocalDateTime;

@Table("chapter")
public class Chapter {
    @Id
    private Long id; // 主键id
    private Long bookId; // 书籍id
    private String chapterUrl; // 书籍章节路径
    private String chapterName; // 书籍章节名称
    private Integer chapterOrderId; // 书籍章节排序
    private String chapterTxt; // 章节内容
    private Long createUserId; // 章节创建用户id
    private String createUserName; // 章节创建用户名称
    private LocalDateTime createTime; // 创建时间
    private Long modifyUserId; // 章节修改用户id
    private String modifyUserName; // 章节修改用户名称
    private LocalDateTime modifyTime; // 修改时间

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getBookId() {
        return bookId;
    }

    public void setBookId(Long bookId) {
        this.bookId = bookId;
    }

    public String getChapterUrl() {
        return chapterUrl;
    }

    public void setChapterUrl(String chapterUrl) {
        this.chapterUrl = chapterUrl;
    }

    public String getChapterName() {
        return chapterName;
    }

    public void setChapterName(String chapterName) {
        this.chapterName = chapterName;
    }

    public Integer getChapterOrderId() {
        return chapterOrderId;
    }

    public void setChapterOrderId(Integer chapterOrderId) {
        this.chapterOrderId = chapterOrderId;
    }

    public String getChapterTxt() {
        return chapterTxt;
    }

    public void setChapterTxt(String chapterTxt) {
        this.chapterTxt = chapterTxt;
    }

    public Long getCreateUserId() {
        return createUserId;
    }

    public void setCreateUserId(Long createUserId) {
        this.createUserId = createUserId;
    }

    public String getCreateUserName() {
        return createUserName;
    }

    public void setCreateUserName(String createUserName) {
        this.createUserName = createUserName;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public Long getModifyUserId() {
        return modifyUserId;
    }

    public void setModifyUserId(Long modifyUserId) {
        this.modifyUserId = modifyUserId;
    }

    public String getModifyUserName() {
        return modifyUserName;
    }

    public void setModifyUserName(String modifyUserName) {
        this.modifyUserName = modifyUserName;
    }

    public LocalDateTime getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(LocalDateTime modifyTime) {
        this.modifyTime = modifyTime;
    }
}