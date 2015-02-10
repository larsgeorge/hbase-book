package com.hbasebook.hush.servlet.security;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.server.UserIdentity;

import com.hbasebook.hush.ResourceManager;
import com.hbasebook.hush.UserManager;
import com.hbasebook.hush.model.User;

/**
 * Implements the Jetty <code>LoginService</code> instance required for server
 * based authentication.
 */
public class HBaseLoginService extends MappedLoginService {
  private final Log LOG = LogFactory.getLog(HBaseLoginService.class);

  public HBaseLoginService(String name) {
    super();
    setName(name);
  }

  @Override
  protected UserIdentity loadUser(String username) {
    try {
      UserManager manager = ResourceManager.getInstance().getUserManager();
      User user = manager.getUser(username);
      String roleString = user.getRoles();
      String[] roles = roleString == null ? null : roleString.split(",");
      return putUser(username, Credential.getCredential(user.getCredentials()),
        roles);
    } catch (Exception e) {
      LOG.error(String.format("Unable to get user '%s'", username), e);
      return null;
    }
  }

  @Override
  protected void loadUsers() throws IOException {
    UserManager manager = ResourceManager.getInstance().getUserManager();
    for (User user : manager.getUsers()) {
      String roleString = user.getRoles();
      String[] roles = roleString == null ? null : roleString.split(",");
      putUser(user.getCredentials(), Credential.getCredential(
        user.getCredentials()), roles);
    }
  }
}
