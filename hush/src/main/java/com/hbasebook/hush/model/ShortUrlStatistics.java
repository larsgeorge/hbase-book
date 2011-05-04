package com.hbasebook.hush.model;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Container class to hold the details for the presentation layer.
 */
public class ShortUrlStatistics {
  private final ShortUrl shortUrl;
  private final TimeFrame timeFrame;
  private final Map<String, NavigableSet<?>> counters = new HashMap<String, NavigableSet<?>>();

  public ShortUrlStatistics(ShortUrl shortUrl, TimeFrame timeFrame) {
    this.shortUrl = shortUrl;
    this.timeFrame = timeFrame;
  }

  public ShortUrl getShortUrl() {
    return shortUrl;
  }

  public TimeFrame getTimeFrame() {
    return timeFrame;
  }

  public NavigableSet<?> getCounters(String name) {
    return counters.get(name);
  }

  public void addCounters(String name, NavigableSet<?> counters) {
    this.counters.put(name, counters);
  }
}