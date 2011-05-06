package com.hbasebook.hush.model;

/**
 * All possible statistics saved in columns in the table.
 */
public enum StatisticsCategory {
  CLICK("cl"), COUNTRY("co");

  private final String postfix;

  StatisticsCategory(String postfix) {
    this.postfix = postfix;
  }

  public String getPostfix() {
    return postfix;
  }

  public byte getCode() {
    return (byte) (ordinal() + 1);
  }

  @Override
  public String toString() {
    return postfix;
  }

  public static StatisticsCategory forPostfix(String postfix) {
    for (StatisticsCategory c : StatisticsCategory.values()) {
      if (c.postfix.equals(postfix)) {
        return c;
      }
    }
    return null;
  }

  public static StatisticsCategory forCode(int code) {
    for (StatisticsCategory c : StatisticsCategory.values()) {
      if (c.ordinal() + 1 == code) {
        return c;
      }
    }
    return null;
  }
}