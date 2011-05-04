package com.hbasebook.hush.model;

/**
 * All possible statistics saved in columns in the table.
 */
public enum Category {
  Click("cl"), Country("co");

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
}