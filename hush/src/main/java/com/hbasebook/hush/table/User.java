package com.hbasebook.hush.table;


public class User {
  private final String username;
  private final String firstName;
  private final String lastName;
  private final String email;
  private final String credentials;
  private final String[] roles;

  public User(String username, String firstName, String lastName, String email,
      String credentials, String[] roles) {
    super();
    this.username = username;
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.credentials = credentials;
    this.roles = roles;
  }

  public String getUsername() {
    return username;
  }

  public String getEmail() {
    return email;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getCredentials() {
    return credentials;
  }

  public String[] getRoles() {
    return roles;
  }
}
