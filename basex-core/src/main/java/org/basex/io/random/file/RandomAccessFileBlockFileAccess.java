package org.basex.io.random.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Implementation of {@link BlockFileAccess} using {@link RandomAccessFile}.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dimitar Popov
 */
final class RandomAccessFileBlockFileAccess extends BlockFileAccess {
  /** File access. */
  private final RandomAccessFile file;

  /**
   * Constructor.
   * @param f file
   */
  RandomAccessFileBlockFileAccess(RandomAccessFile f) {
    file = f;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public void write(Buffer b) throws IOException {
    file.seek(b.getPos());
    file.write(b.getData());
    b.setDirty(false);
  }

  @Override
  public void read(Buffer b, int max) throws IOException {
    long pos = b.getPos();
    file.seek(pos);
    if(pos < file.length()) {
      file.readFully(b.getData(), 0, max);
    }
  }

  @Override
  public void read(Buffer b) throws IOException {
    long pos = b.getPos();
    file.seek(pos);
    file.readFully(b.getData());
  }

  @Override
  public long length() throws IOException {
    return file.length();
  }

  @Override
  public void setLength(long l) throws IOException {
    file.setLength(l);
  }

  @Override
  public FileChannel getChannel() {
    return file.getChannel();
  }
}
