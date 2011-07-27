package org.basex.io.in;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class wraps a {@link BufferInput} reference to a standard input stream.
 * {@code -1} is returned if the end of the stream is reached.
 * The method {@link #curr()} can be called to return the current stream
 * value, which is the first value to be returned, or the most recent value
 * that has been returned by {@link #read()}.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class WrapInputStream extends InputStream {
  /** Input stream. */
  private final InputStream input;
  /** Current value. */
  private int curr;

  /**
   * Constructor.
   * @param in buffer input to be wrapped
   * @throws IOException I/O exception
   */
  public WrapInputStream(final BufferInput in) throws IOException {
    input = in;
    read();
  }

  /**
   * Returns the current value.
   * @return current value
   */
  public int curr() {
    return curr;
  }

  @Override
  public int read() throws IOException {
    final int v = curr;
    if(v == -1) return -1;
    curr = input.read();
    if(curr == 0) curr = -1;
    return v;
  }
}
