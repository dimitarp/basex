package org.basex.io.random;

import static org.basex.util.BlockAccessUtil.*;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.util.BitBuffer;
import org.basex.util.Num;

public class RecordAccess {
  /** Bit mask used to extract the slot number from a record id. */
  public static final long SLOTMASK = (1L << IO.BLOCKPOWER) - 1L;
  /** Header blocks. */
  private final HeaderBlocks headers;
  /** Data blocks. */
  private final DataBlocks data;

  /**
   * Constructor; open a file for data access.
   * @param file file
   * @throws IOException I/O exception
   */
  public RecordAccess(final File file) throws IOException {
    headers = new HeaderBlocks(file);
    data = new DataBlocks(file, headers);
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    return data.select(block(rid), slot(rid));
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    data.delete(block(rid), slot(rid));
  }

  /**
   * Insert a record.
   * @param record record data
   * @return record id
   */
  public long insert(final byte[] record) {
    return data.insert(record);
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    headers.flush();
    data.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    headers.close();
    data.close();
  }

  /**
   * Extract the block index from a record id.
   * @param rid record id
   * @return block index
   */
  private static int block(final long rid) {
    return (int) (rid >>> IO.BLOCKPOWER);
  }

  /**
   * Extract the slot index from a record id.
   * @param rid record id
   * @return slot index
   */
  private static int slot(final long rid) {
    return (int) (rid & SLOTMASK);
  }

  /** Common class for data and header blocks. */
  private abstract static class Blocks {
    /** Size of a block in bits. */
    protected static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;
    /** Underlying data file. */
    protected final BlockManagedDataAccess da;
    /** Dirty flag. */
    protected boolean dirty;
    /** Address of the current block in the underlying storage. */
    protected long addr;

    /**
     * Constructor; open a file for block access.
     * @param file file
     * @throws IOException I/O exception
     */
    public Blocks(final File file) throws IOException {
      da = new BlockManagedDataAccess(file);
    }

    /**
     * Flush cached data to the disk.
     * @throws IOException I/O exception
     */
    public void flush() throws IOException {
      writeMetaData();
      da.flush();
    }

    /**
     * Close file.
     * @throws IOException I/O exception
     */
    public void close() throws IOException {
      if(dirty) flush();
      da.close();
    }

    /**
     * Go to the data block with the specified block id number.
     * @param blockAddr block address
     */
    protected void gotoBlock(final long blockAddr) {
      if(addr == blockAddr) return;
      if(dirty) writeMetaData();
      addr = blockAddr;
      readMetaData();
    }

    /** Read meta data from the current block. */
    protected abstract void readMetaData();

    /** Write meta data to the underlying storage.  */
    protected abstract void writeMetaData();

    /**
     * Read the first 12 bits from 2 bytes.
     * @param b0 first byte
     * @param b1 second byte
     * @return integer with the read 12 bits
     */
    static int readFirst12Bits(final byte b0, final byte b1) {
      return ((b1 & 0x0F) << 8) | ((b0 & 0xFF));
    }

    /**
     * Read the last 12 bits from 2 bytes.
     * @param b0 first byte
     * @param b1 second byte
     * @return integer with the read 12 bits
     */
    static int readLast12Bits(final byte b0, final byte b1) {
      return ((b1 & 0xFF) << 4) | ((b0 & 0xF0) >>> 4);
    }

    /**
     * Write 12 bits to the beginning of 2 bytes.
     * @param d array to write to
     * @param b0 index of the first byte
     * @param b1 index of the second byte
     * @param v integer with the 12 bits to write
     */
    static void writeFirst12Bits(final byte[] d, final int b0,
        final int b1, final int v) {
      d[b0] = (byte) v;
      d[b1] = (byte) ((d[b1] & 0xF0) | ((v >>> 8) & 0x0F));
    }

    /**
     * Write 12 bits to the end of 2 bytes.
     * @param d array to write to
     * @param b0 index of the first byte
     * @param b1 index of the second byte
     * @param v integer with the 12 bits to write
     */
    static void writeLast12Bits(final byte[] d, final int b0,
        final int b1, final int v) {
      d[b0] = (byte) ((d[b1] & 0x0F) | ((v & 0x0F) << 4));
      d[b1] = (byte) (v >>> 4);
    }
  }

  /** Header blocks. */
  private static final class HeaderBlocks extends Blocks {
    /** Size of a block reference in bytes. */
    private static final int REFSIZE = 5;
    /** Size of a block reference in bits. */
    private static final int REFSIZEBITS = REFSIZE * Byte.SIZE;
    /** Number of data blocks per header. */
    private static final int BLOCKS = (BLOCKSIZEBITS - REFSIZEBITS) /
        (REFSIZEBITS + IO.BLOCKPOWER);

    /** Block index in the list of headers. */
    private int num;

    // data stored on disk
    /** Next header block; {@code 0} if the last one. */
    private long next;
    /** Id numbers of blocks managed by this header block. */
    private long[] blocks = new long[BLOCKS];
    /** Free space in each block. */
    int[] free = new int[BLOCKS];

    /**
     * Constructor; open a file for data block access.
     * @param file file
     * @throws IOException I/O exception
     */
    public HeaderBlocks(final File file) throws IOException {
      super(file);
    }

    public int findBlock(final int rsize) {
      // record + record length + slot
      final int size = rsize + Num.length(rsize) + 2;
      // TODO
      return 0;
    }

    /**
     * Get the address of a data block with a given index.
     * @param blockIndex data block index
     * @return block address
     */
    public long getBlockAddr(final int blockIndex) {
      gotoHeaderBlock(headerIndex(blockIndex));
      return blocks[blockIndex % BLOCKS];
    }

    @Override
    protected void readMetaData() {
      da.gotoBlock(addr);
      final Buffer buf = da.buffer(false);
      final byte[] data = buf.data;

      // NEXT is stored first
      next = da.read5();

      // BLOCK ADDRESSES are stored next
      for(int i = 0; i < BLOCKS; ++i) blocks[i] = da.read5();

      // FREE are stored next
      int p = BLOCKS;
      final int n = BLOCKS - 1;
      for(int i = 0; i < n; i += 2) {
        free[i] = readFirst12Bits(data[p], data[++p]);
        free[i + 1] = readLast12Bits(data[p], data[++p]);
      }
      free[n] = readFirst12Bits(data[p], data[++p]);
    }

    @Override
    protected void writeMetaData() {
      da.gotoBlock(addr);
      final Buffer buf = da.buffer(false);

      final byte[] data = buf.data;
      buf.dirty = true;

      // NEXT is stored first
      da.write5(next);

      // BLOCK ADDRESSES are stored next
      for(int i = 0; i < BLOCKS; ++i) da.write5(blocks[i]);

      // FREE are stored next
      int p = BLOCKS;
      final int n = BLOCKS - 1;
      for(int i = 0; i < n; i += 2) {
        writeFirst12Bits(data, p, ++p, free[i]);
        writeLast12Bits(data, p, ++p, free[i + 1]);
      }
      writeFirst12Bits(data, p, ++p, free[n]);

      dirty = false;
    }

    /**
     * Go to a header block.
     * @param n header block index
     */
    private void gotoHeaderBlock(final int n) {
      if(num == n) return;
      // go to the first header, if n is smaller than the current header index
      if(num > n) {
        gotoBlock(0L);
        num = 0;
      }
      // scan headers, until the required index is reached
      for(; num < n; ++num) gotoBlock(next);
    }

    /**
     * Get the index in the list of headers of the header block, which has the
     * address of the block with the given number.
     * @param blockIndex block number
     * @return header block index
     */
    private static int headerIndex(final int blockIndex) {
      return blockIndex / BLOCKS;
    }
  }

  /** Data blocks. */
  private static final class DataBlocks extends Blocks {
    /** Size of an offset within a block in bits. */
    private static final int OFFSET_SIZE = IO.BLOCKPOWER;
    /** Maximal number of records in a data block. */
    private static final int MAX_RECORDS = IO.BLOCKSIZE >>> 1;
    /** Empty slot marker. */
    private static final int NIL = (int) SLOTMASK;

    /** Header blocks. */
    private final HeaderBlocks headers;

    /** Size of the meta data of the current block. */
    private int metaDataSize;

    // fields stored in the block:
    /** Data area size (in bytes). */
    private int size;
    /** Number of slots. */
    private int num;
    /** Slots with record offsets in the data area. */
    private final int[] slots = new int[MAX_RECORDS];

    /**
     * Constructor; open a file for data block access.
     * @param f file
     * @param h header blocks
     * @throws IOException I/O exception
     */
    public DataBlocks(final File f, final HeaderBlocks h) throws IOException {
      super(f);
      headers = h;
    }

    /**
     * Retrieve the content of a record.
     * @param blockIndex block index
     * @param slot slot index
     * @return data stored in the record
     */
    public byte[] select(final int blockIndex, final int slot) {
      gotoBlock(headers.getBlockAddr(blockIndex));

      da.off = slots[slot];
      return da.readToken();
    }

    /**
     * Delete a record.
     * @param blockIndex block address
     * @param slot slot index
     */
    public void delete(final int blockIndex, final int slot) {
      gotoBlock(headers.getBlockAddr(blockIndex));

      // read the record length
      da.off = slots[slot];
      int len = da.readNum();
      len += Num.length(len);
      // decrease size
      size -= len;

      if(slot == num) {
        // the record from the last slot is deleted: decrease number of slots
        --num;
        // decrease the size of the meta data
        metaDataSize -= OFFSET_SIZE;
        adjustFreeSpace(blockIndex); // TODO
      }

      headers.free[blockIndex] += len;
      headers.dirty = true;

      slots[slot] = NIL;
      dirty = true;
    }

    /**
     * Insert a record.
     * @param record record data
     * @return record id
     */
    public long insert(final byte[] record) {
      final int blockIndex = headers.findBlock(record.length);
      gotoBlock(headers.getBlockAddr(blockIndex));

      // write the record data
      final int len = Num.length(record.length) + record.length;
      final int off = da.off = allocate(len);
      da.writeToken(record);
      // increase size
      size += len;

      final int slot = findEmptySlot();
      if(slot == num) {
        // new slot will be allocated
        ++num;
        // increase the size of the meta data
        metaDataSize += OFFSET_SIZE;
        adjustFreeSpace(blockIndex); // TODO
      }

      headers.free[blockIndex] -= len;
      headers.dirty = true;

      slots[slot] = off;
      dirty = true;

      return ((long) blockIndex << IO.BLOCKPOWER) & slot;
    }

    @Override
    protected void readMetaData() {
      da.gotoBlock(addr);
      final byte[] data = da.buffer(false).data;

      // DATA AREA SIZE is stored at 12 bits the end of each block
      size = readFirst12Bits(data[IO.BLOCKSIZE - 1], data[IO.BLOCKSIZE - 2]);
      // NUMBER OF SLOTS is stored the next 12 bits
      num = readLast12Bits(data[IO.BLOCKSIZE - 2], data[IO.BLOCKSIZE - 3]);

      // SLOTS are stored next
      int p = IO.BLOCKSIZE - 4;
      final boolean odd = (num & 1) == 1;
      final int n = odd ? num - 1 : num;
      for(int i = 0; i < n; i += 2) {
        slots[i] = readFirst12Bits(data[p], data[--p]);
        slots[i + 1] = readLast12Bits(data[p], data[--p]);
      }
      if(odd) slots[n] = readFirst12Bits(data[p], data[--p]);

      metaDataSize = (num + 2) * OFFSET_SIZE;
    }

    @Override
    protected void writeMetaData() {
      da.gotoBlock(addr);
      final Buffer buf = da.buffer(false);

      final byte[] data = buf.data;
      buf.dirty = true;

      // DATA AREA SIZE is stored at 12 bits the end of each block
      writeFirst12Bits(data, IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2, size);
      // NUMBER OF SLOTS is stored the next 12 bits
      writeLast12Bits(data, IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3, num);

      // SLOTS are stored next
      int p = IO.BLOCKSIZE - 4;
      final boolean odd = (num & 1) == 1;
      final int n = odd ? num - 1 : num;
      for(int i = 0; i < n; i += 2) {
        writeFirst12Bits(data, p, --p, slots[i]);
        writeLast12Bits(data, p, --p, slots[i + 1]);
      }
      if(odd) writeFirst12Bits(data, p, --p, slots[n]);

      dirty = false;
    }

    /**
     * Find an empty slot; if none is available, create a new one at the end.
     * @return index of the empty slot
     */
    private int findEmptySlot() {
      for(int r = 0; r < num; ++r) if(slots[r] == NIL) return r;
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
      return size;
    }

    /** Compact records in a contiguous area. */
    private void compact() {
      int pos = 0;
      for(int i = 0; i < num; ++i) {
        // read the length of the record (size + data)
        da.off = slots[i];
        int len = da.readNum();
        len += Num.length(len);

        if(slots[i] > pos) {
          // there is unused space: shift the record (size + data) forwards
          da.off = slots[i];
          final byte[] record = da.readBytes(len);
          da.off = pos;
          da.writeBytes(record);
          slots[i] = pos;
        } else if(slots[i] < pos) {
          throw new RuntimeException("Not expected");
        }
        // next position should be right after the current record
        pos += len;
      }
      // adjust the size value
      size = pos;
    }

    private void adjustFreeSpace(final int blockIndex) {
      headers.free[blockIndex] = IO.BLOCKSIZE - size - (int) divRoundUp(metaDataSize, 8L);
      headers.dirty = true;
    }
  }
}
