package com.hbasebook.hush.model;

import java.util.Date;
import java.util.NavigableMap;

/**
 * Container class to hold the details for the presentation layer.
 */
public class ShortUrlStatistics {
  private final ShortUrl shortUrl;
  private final NavigableMap<Date, Double> clicks;
  private final TimeFrame timeFrame;

  public ShortUrlStatistics(ShortUrl shortUrl,
      NavigableMap<Date, Double> clicks, TimeFrame timeFrame) {
    this.shortUrl = shortUrl;
    this.clicks = clicks;
    this.timeFrame = timeFrame;
  }

  public ShortUrl getShortUrl() {
    return shortUrl;
  }

  public NavigableMap<Date, Double> getClicks() {
    return clicks;
  }

  public TimeFrame getTimeFrame() {
    return timeFrame;
  }
}