package org.example;

import java.io.Serializable;

public class CouchbaseClusterInfo implements Serializable {

  private final String host;
  private final String user;
  private final String password;

  public CouchbaseClusterInfo(String host, String user, String password) {
    this.host = host;
    this.user = user;
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }
}
