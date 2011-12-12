package org.basex.io.random;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.util.Num;
import org.basex.util.Token;

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

  /** Class representing a data block. */
  private final class DataBlock {
    /** Maximal number of records in a data block. */
    public static final int MAX_RECORDS = IO.BLOCKSIZE >>> 1;
    /** Empty slot marker. */
    public static final int NIL = (1 << IO.BLOCKPOWER) - 1;

    /** Block id number. */
    int id;
    /** Free space in bytes. */
    int free;

    // fields stored in the block:
    /** Data area size (in bytes). */
    int size;
    /** Number of records. */
    int num;
    /** Record offsets in the data area. */
    final int[] offsets = new int[MAX_RECORDS];

    /** Write the meta data to the current block. */
    public void writeMetaData() {
      // TODO
    }

    /** Read the meta data from the current block. */
    public void readMetaData() {
      // TODO
    }

    /**
     * Add a new record to the block.
     * @param data data record
     * @return record number within the block
     */
    public int insert(final byte[] data) {
      // write the record data
      final int off = allocate(Num.length(data.length) + data.length);
      da.off = off;
      da.writeToken(data);

      final int record = findEmptySlot();
      if(record == num) {
        // TODO: new slot will be allocated: the free space must be adjusted
      }

      offsets[record] = off;
      ++num;

      return record;
    }

    /**
     * Delete a record from the block.
     * @param record record number within the block
     */
    public void delete(final int record) {
      if(record == num--) {
        da.off = offsets[record];
        int len = da.readNum();
        len += Num.length(len);
        // TODO: free space occupied by the slot
        // decrease size
        size -= len;
        // increase free
        free += len;
      }
      offsets[record] = NIL;
    }

    /**
     * Retrieve the content of a record.
     * @param record record number within the block
     * @return data stored in the record
     */
    public byte[] select(final int record) {
      da.off = offsets[record];
      return da.readToken();
    }

    /**
     * Find an empty slot; if none is available, create a new one at the end.
     * @return index of the empty slot
     */
    private int findEmptySlot() {
      for(int r = 0; r < num; ++r) if(offsets[r] == NIL) return r;
      return num;
    }

    /**
     * Allocate a given number of bytes from the free space area.
     * @param l number of bytes
     * @return offset within the block where the bytes have been allocated
     */
    private int allocate(final int l) {
      // re-organize records, if not enough space
      if(l > free) compact();

      // allocate at the end of the data area
      final int off = size;
      size += l;
      free -= l;
      return off;
    }

    /** Compact records in a contiguous area. */
    private void compact() {
      int pos = 0;
      for(int i = 0; i < num; ++i) {
        // read the length of the record (size + data)
        da.off = offsets[i];
        int len = da.readNum();
        len += Num.length(len);

        if(offsets[i] > pos) {
          // there is unused space: shift the record (size + data) forwards
          da.off = offsets[i];
          final byte[] record = da.readBytes(len);
          da.off = pos;
          da.writeBytes(record);
          offsets[i] = pos;
        } else if(offsets[i] < pos) {
          throw new RuntimeException("Not expected");
        }
        // next position should be right after the current record
        pos += len;
      }
      // adjust the free space value
      free += size - pos;
      // adjust the size value
      size = pos;
    }
  }

  private static final class HeaderBlock {
    /** Block id number. */
    int id;
    /** Next header block; {@code 0} if the last one. */
    int next;
  }

  public static final int REFSIZE = 5;
  /** Number of data blocks per segment. */
  public static final int SEGMENTBLOCKS =
      (Byte.SIZE * IO.BLOCKSIZE) / (Byte.SIZE * REFSIZE + 1);

  public static final long SLOTMASK = 0xFFFL;

  final BlockManagedDataAccess da;

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
