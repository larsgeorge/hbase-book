package com.hbasebook.hush.model;

import java.util.List;

public class ShortDomain {
  private final String shortDomain;
  private final List<String> domains;

  public ShortDomain(String shortDomain, List<String> domains) {
    super();
    this.shortDomain = shortDomain;
    this.domains = domains;
  }

  public String getShortDomain() {
    return shortDomain;
  }

  public List<String> getDomains() {
    return domains;
  }
}
