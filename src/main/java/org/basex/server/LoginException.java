package org.basex.server;

import static org.basex.core.Text.*;
import java.io.IOException;

/**
 * This exception is thrown if a wrong user/password combination was specified.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class LoginException extends IOException {
  /**
   * Constructor.
   */
  LoginException() {
    super(SERVERDENIED);
  }
}
