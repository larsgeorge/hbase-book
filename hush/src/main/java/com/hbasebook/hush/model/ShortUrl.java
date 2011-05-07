package com.hbasebook.hush.model;

import com.hbasebook.hush.ResourceManager;

import java.io.IOException;

public class ShortUrl {
  private final String id;
  private final String domain;
  private final String longUrl;
  private final String refShortId;
  private final String user;

  public ShortUrl(String id, String domain, String longUrl,
      String refShortId, String user) {
    this.id = id;
    this.domain = domain;
    this.longUrl = longUrl;
    this.refShortId = refShortId;
    this.user = user;
  }

  public String getId() {
    return id;
  }

  public String getDomain() {
    return domain;
  }

  public String getLongUrl() {
    return longUrl;
  }

  public String getRefShortId() {
    return refShortId;
  }

  public String getUser() {
    return user;
  }

  @Override
  public String toString() {
    int port = ResourceManager.getHushPort();
    return "http://" + domain + (port != 80 ? ":" + port : "") + "/" + id;
  }
}
