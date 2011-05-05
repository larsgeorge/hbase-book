package com.hbasebook.hush.model;

public class LongUrl {
  private final String url;
  private final String shortId;

  public LongUrl(String url, String shortId) {
    this.url = url;
    this.shortId = shortId;
  }

  public String getUrl() {
    return url;
  }

  public String getShortId() {
    return shortId;
  }
}
