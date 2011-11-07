package org.basex.io.random;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;

/**
 * Implementation of {@link DataAccess}, which in addition manages the empty
 * blocks.
 *
 * The file is divided into segments. Each segment has a header block, which
 * manages the following blocks.
 * @author dimitar
 *
 */
public class BlockDataAccess {
  public static final int REFSIZE = 5;
  /** Number of data blocks per segment. */
  public static final int SEGMENTBLOCKS =
      (Byte.SIZE * IO.BLOCKSIZE) / (Byte.SIZE * REFSIZE + 1);

  public static final long SLOTMASK = 0xFFFL;

  private final BlockManagedDataAccess da;

  public BlockDataAccess(final File f) throws IOException {
    da = new BlockManagedDataAccess(f);
  }

  public long insert(final byte[] data) {
    // TODO: not implemented
    return 0L;
  }

  public void delete(final long rid) {
    // TODO: not implemented
  }

  public byte[] select(final long rid) {
    // TODO: not implemented
    final int b = blockid(rid);
    final int s = slotid(rid);
    da.gotoBlock(b);
    return da.readToken();
  }

  public void flush() throws IOException {
    da.flush();
  }

  public void close() throws IOException {
    da.close();
  }

  public static int blockid(final long rid) {
    return (int) (rid >>> IO.BLOCKPOWER);
  }

  public static int slotid(final long rid) {
    return (int) (rid & SLOTMASK);
  }
}
