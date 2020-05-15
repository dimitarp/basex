package org.basex.util;

import static org.basex.util.Prop.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;

import org.basex.io.*;

/**
 * This class contains environment properties which are used all around the project.
 *
 * @author BaseX Team 2005-20, BSD License
 * @author Christian Gruen
 */
public final class Env {
  /** Returns the system's default encoding. */
  public static final String ENCODING = System.getProperty("file.encoding");
  /** System's temporary directory. */
  public static final String TEMPDIR = dir(System.getProperty("java.io.tmpdir"));
  /** Project home directory. */
  public static final String HOMEDIR;

  // determine project home directory for storing property files and directories...
  static {
    // check system property 'org.basex.path'
    String homedir = System.getProperty(PATH);
    // check if current working directory contains configuration file
    if(homedir == null) homedir = configDir(System.getProperty("user.dir"));
    // check if application directory contains configuration file
    if(homedir == null) homedir = configDir(applicationDir(Prop.LOCATION));
    // fallback: choose home directory (linux: check HOME variable, GH-773)
    if(homedir == null) {
      final String home = WIN ? null : System.getenv("HOME");
      homedir = dir(home != null ? home : System.getProperty("user.home")) + PROJECT_NAME;
    }
    HOMEDIR = dir(homedir);
  }
  
  
  /**
   * Checks if one of the files .basexhome or .basex are found in the specified directory.
   * @param dir directory (can be {@code null})
   * @return configuration directory (can be {@code null})
   */
  private static String configDir(final String dir) {
    if(dir != null) {
      final String home = IO.BASEXSUFFIX + "home";
      final IOFile file = new IOFile(dir, home);
      if(file.exists() || new IOFile(dir, IO.BASEXSUFFIX).exists()) return dir;
    }
    return null;
  }

  /**
   * Returns the application directory.
   * @param location location of application
   * @return application directory (can be {@code null})
   */
  private static String applicationDir(final URL location) {
    try {
      if(location != null) return new IOFile(Paths.get(location.toURI()).toString()).dir();
    } catch(final Exception ex) {
      Util.stack(ex);
    }
    return null;
  }

  /**
   * Attaches a directory separator to the specified directory string.
   * @param path directory path
   * @return directory string
   */
  private static String dir(final String path) {
    return path.isEmpty() || Strings.endsWith(path, '/') || Strings.endsWith(path, '\\') ?
      path : path + File.separator;
  }
}