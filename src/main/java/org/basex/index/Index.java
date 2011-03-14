package org.basex.index;

import java.io.IOException;

/**
 * This interface defines the methods which have to be implemented by an index
 * structure.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public interface Index {
  /**
   * Returns information on the index structure.
   * @return info
   */
  byte[] info();

  /**
   * Returns an iterator for the index results.
   * @param tok token to be found
   * @return sorted pre values for the token
   */
  IndexIterator iter(final IndexToken tok);

  /**
   * Returns the (approximate/estimated) number of pres for the specified token.
   * @param tok token to be found
   * @return number of pres
   */
  int count(final IndexToken tok);

  /**
   * Closes the index.
   * @throws IOException I/O exception
   */
  void close() throws IOException;
}
