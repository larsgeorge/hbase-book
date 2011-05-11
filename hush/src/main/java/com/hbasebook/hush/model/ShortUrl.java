package com.hbasebook.hush.model;

import com.hbasebook.hush.ResourceManager;

/**
 * Container for all the information needed for a shortened URL.
 */
public class ShortUrl {
  private final String id;
  private final String domain;
  private final String longUrl;
  private final String refShortId;
  private final String user;
  private final long clicks;

  public ShortUrl(String id, String domain, String longUrl,
    String refShortId, String user) {
    this(id, domain, longUrl, refShortId, user, 0);
  }

  public ShortUrl(String id, String domain, String longUrl,
    String refShortId, String user, long clicks) {
    this.id = id;
    this.domain = domain;
    this.longUrl = longUrl;
    this.refShortId = refShortId;
    this.user = user;
    this.clicks = clicks;
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

  public long getClicks() {
    return clicks;
  }

  @Override
  public String toString() {
    int port = ResourceManager.getHushPort();
    return "http://" + domain + (port != 80 ? ":" + port : "") + "/" + id;
  }
}
