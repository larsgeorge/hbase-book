package com.hbasebook.hush.table;

import java.util.HashSet;
import java.util.Set;

public class User {
  public static final String USER_ROLE = "user";
  public static final String ADMIN_ROLE = "admin";

  private final String username;
  private final String firstName;
  private final String lastName;
  private final String email;
  private final String credentials;
  private final String roles;
  private transient Set<String> roleSet = null;

  public User(String username, String firstName, String lastName, String email,
      String credentials, String roles) {
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

  public String getRoles() {
    return roles;
  }

  public Set<String> getRoleSet() {
    if (roleSet == null) {
      roleSet = new HashSet<String>();
      if (roles != null) {
        for (String role : roles.split(",")) {
          roleSet.add(role.trim());
        }
      }
    }
    return roleSet;
  }

  public boolean isAdmin() {
    return getRoleSet().contains(ADMIN_ROLE);
  }
}
