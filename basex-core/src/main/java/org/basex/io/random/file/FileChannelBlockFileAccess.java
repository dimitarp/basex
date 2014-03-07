package org.basex.io.random.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * {@link BlockFileAccess} implementation using {@link FileChannel}.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
final class FileChannelBlockFileAccess extends BlockFileAccess {
  /** File channel. */
  private final FileChannel file;

  /**
   * Constructor.
   * @param f file channel
   */
  FileChannelBlockFileAccess(final FileChannel f) {
    file = f;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public void write(final Buffer b) throws IOException {
    ByteBuffer buffer = b.getByteBuffer();
    buffer.rewind();
    file.write(buffer, b.getPos());
    buffer.rewind();
    b.setDirty(false);
  }

  @Override
  public void read(final Buffer b, final int max) throws IOException {
    ByteBuffer buffer = b.getByteBuffer();
    buffer.rewind();
    file.read(buffer, b.getPos());
    buffer.rewind();
  }

  @Override
  public void read(final Buffer b) throws IOException {
    ByteBuffer buffer = b.getByteBuffer();
    buffer.rewind();
    file.read(buffer, b.getPos());
    buffer.rewind();
  }

  @Override
  public long length() throws IOException {
    return file.size();
  }

  @Override
  public void setLength(final long l) throws IOException {
    file.truncate(l);
  }

  @Override
  public FileChannel getChannel() {
    return file;
  }
}
