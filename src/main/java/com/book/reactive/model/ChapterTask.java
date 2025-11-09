package com.book.reactive.model;

import java.io.Serializable;

/**
 * 章节任务实体类，用于RabbitMQ消息传输
 */
public class ChapterTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long bookId;
    private String chapterUrl;
    private String chapterName;
    private Integer chapterOrderId;
    private Long websiteId;
    private String websiteUrl;

    // getter and setter
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

    public Long getWebsiteId() {
        return websiteId;
    }

    public void setWebsiteId(Long websiteId) {
        this.websiteId = websiteId;
    }

    public String getWebsiteUrl() {
        return websiteUrl;
    }

    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }

    @Override
    public String toString() {
        return "ChapterTask{" +
                "bookId=" + bookId +
                ", chapterUrl='" + chapterUrl + '\'' +
                ", chapterName='" + chapterName + '\'' +
                ", chapterOrderId=" + chapterOrderId +
                ", websiteId=" + websiteId +
                ", websiteUrl='" + websiteUrl + '\'' +
                '}';
    }
}