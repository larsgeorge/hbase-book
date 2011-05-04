package com.hbasebook.hush.model;

public class LongUrl {
  private final String url;
  private final String shortId;
  private final String user;

  public LongUrl(String url, String shortId, String user) {
    this.url = url;
    this.shortId = shortId;
    this.user = user;
  }

  public String getUrl() {
    return url;
  }

  public String getShortId() {
    return shortId;
  }

  public String getUser() {
    return user;
  }
}
