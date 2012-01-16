package org.basex.io.random;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.io.random.RecordDataAccess.HeaderBlock;
import org.basex.util.BitBuffer;
import org.basex.util.Num;

public class RecordAccess {
  /** Bit mask used to extract the slot number from a record id. */
  public static final long SLOTMASK = (1L << IO.BLOCKPOWER) - 1L;

  private final HeaderBlocks headers;
  private final DataBlocks data;

  /**
   * Constructor; open a file for data access.
   * @param file file
   * @throws IOException I/O exception
   */
  public RecordAccess(final File file) throws IOException {
    headers = new HeaderBlocks(file);
    data = new DataBlocks(file);
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    return data.select(headers.getBlockAddr(block(rid)), slot(rid));
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int blockIndex = block(rid);
    final long blockAddr = headers.getBlockAddr(blockIndex);

    data.delete(blockAddr, slot(rid));
  }

  /**
   * Insert a record.
   * @param record record data
   * @return record id
   */
  public long insert(final byte[] record) {
    final int blockIndex = headers.findBlock(record.length);
    final long blockAddr = headers.getBlockAddr(blockIndex);

    final int slot = data.insert(blockAddr, record);

    return ((long) blockIndex << IO.BLOCKPOWER) & slot;
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

  private abstract static class Blocks {
    /** Size of a block in bits. */
    protected static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;
    /** Underlying data file. */
    protected final BlockManagedDataAccess da;

    protected final BitBuffer bitAccess;

    protected boolean dirty;

    protected long addr;

    /**
     * Constructor; open a file for block access.
     * @param file file
     * @throws IOException I/O exception
     */
    public Blocks(final File file) throws IOException {
      da = new BlockManagedDataAccess(file);
      bitAccess = new BitBuffer(BLOCKSIZEBITS);
    }

    /**
     * Flush cached data to the disk.
     * @throws IOException I/O exception
     */
    public void flush() throws IOException {
      flushMetaData();
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
      if(dirty) flushMetaData();
      da.gotoBlock(blockAddr);
      addr = blockAddr;
      readMetaData();
    }

    /** Read meta data from the current block. */
    protected abstract void readMetaData();

    /** Write meta data to the {@link #bitAccess} buffer. */
    protected abstract void writeMetaData();

    /** Write meta data to the underlying storage.  */
    private void flushMetaData() {
      writeMetaData();

      da.gotoBlock(addr);
      final Buffer fileBuffer = da.buffer(false);
      bitAccess.serialize(fileBuffer.data);
      fileBuffer.dirty = true;

      dirty = false;
    }
  }

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
    private int[] free = new int[BLOCKS];

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

    public void setFreeSpace(final int blockIndex, final int size) {
      gotoHeaderBlock(headerIndex(blockIndex));
      free[blockIndex % BLOCKS] = size;
      dirty = true;
    }

    @Override
    protected void readMetaData() {
      // NEXT is stored first
      next = da.read5();

      // BLOCK ADDRESSES are stored next
      for(int i = 0; i < BLOCKS; ++i) blocks[i] = da.read5();

      // FREE are stored next
      final byte[] data = da.buffer(false).data;
      bitAccess.init(data, (BLOCKS + 1) << 3, data.length);

      long pos = 0L;
      for(int i = 0; i < BLOCKS; ++i) {
        free[i] = (int) bitAccess.read(pos, IO.BLOCKPOWER);
        pos += IO.BLOCKPOWER;
      }
    }

    @Override
    protected void writeMetaData() {
      // NEXT is stored first
      da.write5(next);

      // BLOCK ADDRESSES are stored next
      for(int i = 0; i < BLOCKS; ++i) da.write5(blocks[i]);

      // FREE are stored next
      long pos = 0L;
      for(int i = 0; i < free.length; ++i) {
        bitAccess.write(pos, IO.BLOCKPOWER, free[i]);
        pos += IO.BLOCKPOWER;
      }
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

  private static final class DataBlocks extends Blocks {
    /** Size of an offset within a block in bits. */
    private static final int OFFSET_SIZE = IO.BLOCKPOWER;
    /** Maximal number of records in a data block. */
    private static final int MAX_RECORDS = IO.BLOCKSIZE >>> 1;
    /** Empty slot marker. */
    private static final int NIL = (int) SLOTMASK;

    // fields stored in the block:
    /** Data area size (in bytes). */
    private int size;
    /** Number of slots. */
    private int num;
    /** Slots with record offsets in the data area. */
    private final int[] slots = new int[MAX_RECORDS];

    /**
     * Constructor; open a file for data block access.
     * @param file file
     * @throws IOException I/O exception
     */
    public DataBlocks(final File file) throws IOException {
      super(file);
    }

    /**
     * Retrieve the content of a record.
     * @param blockAddr block address
     * @param slot slot index
     * @return data stored in the record
     */
    public byte[] select(final long blockAddr, final int slot) {
      gotoBlock(blockAddr);

      da.off = slots[slot];
      return da.readToken();
    }

    /**
     * Delete a record.
     * @param blockAddr block address
     * @param slot slot index
     */
    public void delete(final long blockAddr, final int slot) {
      gotoBlock(blockAddr);

      if(slot == num) {
        // the record from the last slot is deleted
        da.off = slots[slot];
        int len = da.readNum();
        len += Num.length(len);
        // decrease size
        size -= len;
        // increase free
        free += len;
        // TODO: free the space occupied by the slot
        --num;
      }
      slots[slot] = NIL;
      dirty = true;
    }

    /**
     * Insert a record.
     * @param blockAddr block address
     * @param record record data
     * @return slot where the record was inserted
     */
    public int insert(final long blockAddr, final byte[] record) {
      gotoBlock(blockAddr);

      // write the record data
      final int off = allocate(Num.length(record.length) + record.length);
      da.off = off;
      da.writeToken(record);

      final int slot = findEmptySlot();
      if(slot == num) {
        // TODO: new slot will be allocated: the free space must be adjusted
        ++num;
      }

      slots[slot] = off;

      dirty = true;

      return slot;
    }

    @Override
    protected void readMetaData() {
      // TODO: meta-data is small => don't copy the whole block!
      bitAccess.init(da.buffer(false).data);

      long pos = BLOCKSIZEBITS - 1L;

      // DATA AREA SIZE is stored at the end of each block
      size = (int) bitAccess.read(pos, OFFSET_SIZE);
      pos -= OFFSET_SIZE;

      // NUMBER OF SLOTS is stored next
      // FREE are stored next
      num = (int) bitAccess.read(pos, 10);
      pos -= 10;

      // SLOTS are stored next
      for(int i = 0; i < num; ++i) {
        slots[i] = (int) bitAccess.read(pos, OFFSET_SIZE);
        pos -= OFFSET_SIZE;
      }

      free = (pos >>> 3) - size;
    }

    @Override
    protected void writeMetaData() {
      int pos = BLOCKSIZEBITS - 1;

      // DATA AREA SIZE is stored at the end of each block
      bitAccess.write(pos, OFFSET_SIZE, size);
      pos -= OFFSET_SIZE;

      // NUMBER OF SLOTS is stored next
      bitAccess.write(pos, 10, num);
      pos -= 10;

      // SLOTS are stored next
      for(int i = 0; i < num; ++i) {
        bitAccess.write(pos, OFFSET_SIZE, slots[i]);
        pos -= OFFSET_SIZE;
      }
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
      // adjust the free space value
      free += size - pos;
      // adjust the size value
      size = pos;
    }
  }
}
