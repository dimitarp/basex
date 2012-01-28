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
@Deprecated
public class BlockDataAccessOld {

  /** Class representing a data block. */
  private static final class DataBlock {
    /** Maximal number of records in a data block. */
    public static final int MAX_RECORDS = 1 << 11;
    /** Empty slot marker. */
    @SuppressWarnings("unused")
    public static final int NIL = MAX_RECORDS - 1;

    /** Block number. */
    int block;
    /** Data area size (in bytes). */
    int size;
    /** Number of records. */
    int num;
    /** Record offsets in the data area. */
    final int[] offsets = new int[MAX_RECORDS];

    /**
     * Add a new record to the block.
     * @param record data record
     * @return record number within the block
     */
    public int insert(final byte[] record) {
      // TODO
      return 0;
    }

    /**
     * Delete a record from the block.
     * @param record record number within the block
     */
    public void delete(final int record) {
      // TODO
    }

    /**
     * Retrieve the content of a record.
     * @param record record number within the block
     * @return data stored in the record
     */
    public byte[] select(final int record) {
      // TODO
      return null;
    }
  }

  private static final class HeaderBlock {
    /** Block number. */
    int block;
    /** Next header block; {@code 0} if the last one. */
    int next;
    
  }

  public static final int REFSIZE = 5;
  /** Number of data blocks per segment. */
  public static final int SEGMENTBLOCKS =
      (Byte.SIZE * IO.BLOCKSIZE) / (Byte.SIZE * REFSIZE + 1);

  public static final long SLOTMASK = 0xFFFL;

  private final BlockDataAccess da;

  public BlockDataAccessOld(final File f) throws IOException {
    da = new BlockDataAccess(f);
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
