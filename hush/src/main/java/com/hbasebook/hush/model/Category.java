package com.hbasebook.hush.model;

/**
 * All possible statistics saved in columns in the table.
 */
public enum Category {
  CLICK("cl"), COUNTRY("co");

  private final String postfix;

  Category(String postfix) {
    this.postfix = postfix;
  }

  public String getPostfix() {
    return postfix;
  }

  @Override
  public String toString() {
    return postfix;
  }

  public static Category forPostfix(String postfix) {
    for (Category c : Category.values()) {
      if (c.postfix.equals(postfix)) {
        return c;
      }
    }
    return null;
  }
}