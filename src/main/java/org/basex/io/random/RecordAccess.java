package org.basex.io.random;

import static org.basex.util.BlockAccessUtil.*;

import java.io.File;
import java.io.IOException;

import org.basex.io.IO;
import org.basex.util.Num;

/**
 * Record access.
 * @author dpopov
 *
 */
public class RecordAccess extends Blocks {
  /** Bit mask used to extract the slot number from a record id. */
  private static final long SLOTMASK = (1L << IO.BLOCKPOWER) - 1L;

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
   * @throws IOException I/O exception
   */
  public RecordAccess(final File f) throws IOException {
    super(f);
    headers = new HeaderBlocks(this);
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    headers.flush();
  }

  @Override
  public void close() throws IOException {
    super.close();
    headers.close();
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    gotoBlock(headers.getBlockAddr(block(rid)));

    da.off = slots[slot(rid)];
    return da.readToken();
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int slot = slot(rid);
    final int block = block(rid);
    final int blockIndex = block % HeaderBlocks.BLOCKS;
    gotoBlock(headers.getBlockAddr(block));

    // read the record length
    da.off = slots[slot];
    int len = da.readNum();
    len += Num.length(len);
    // decrease size
    size -= len;

    if(slot == num) {
      // decrease the size of the meta data
      metaDataSize -= OFFSET_SIZE;
      // if slot is even, then 1 byte will be freed; else 2 bytes
      headers.used[blockIndex] -= (num & 1) == 0 ? 1 : 2;
      // the record from the last slot is deleted: decrease number of slots
      --num;
    }

    headers.used[blockIndex] -= len;
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
    final int block = headers.findBlock(record.length);
    final int blockIndex = block % HeaderBlocks.BLOCKS;
    gotoBlock(headers.blocks[blockIndex]);

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
      // if slot is even, then 1 new byte will be allocated; else 2 bytes
      headers.used[blockIndex] += (num & 1) == 0 ? 1 : 2;
    }

    headers.used[blockIndex] += len;
    headers.dirty = true;

    slots[slot] = off;
    dirty = true;

    return (((long) block) << IO.BLOCKPOWER) | slot;
  }

  @Override
  protected void readMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    size = da.readLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2);
    // NUMBER OF SLOTS is stored the next 12 bits
    num = da.readHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      slots[i] = da.readLow12Bits(p, --p);
      slots[i + 1] = da.readHigh12Bits(p, --p);
    }
    if(odd) slots[n] = da.readLow12Bits(p, --p);

    metaDataSize = (num + 2) * OFFSET_SIZE;
  }

  @Override
  protected void writeMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    da.writeLow12Bits(IO.BLOCKSIZE - 1, IO.BLOCKSIZE - 2, size);
    // NUMBER OF SLOTS is stored the next 12 bits
    da.writeHigh12Bits(IO.BLOCKSIZE - 2, IO.BLOCKSIZE - 3, num);

    // SLOTS are stored next
    int p = IO.BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      da.writeLow12Bits(p, --p, slots[i]);
      da.writeHigh12Bits(p, --p, slots[i + 1]);
    }
    if(odd) da.writeLow12Bits(p, --p, slots[n]);
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
    if(l > IO.BLOCKSIZE - size - (int) divRoundUp(metaDataSize, 8)) compact();
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
}

/** Common class for data and header blocks. */
abstract class Blocks {
  /** Size of a block in bits. */
  static final int BLOCKSIZEBITS = IO.BLOCKSIZE << 3;

  /** Underlying data file. */
  protected final BlockManagedDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;

  public Blocks(final BlockManagedDataAccess data) {
    da = data;
  }

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
    dirty = false;
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
    if(dirty) {
      writeMetaData();
      dirty = false;
    }
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
    d[b0] = (byte) ((d[b0] & 0x0F) | ((v & 0x0F) << 4));
    d[b1] = (byte) (v >>> 4);
  }
}

/** Header blocks. */
class HeaderBlocks extends Blocks {
  /** Size of a block reference in bytes. */
  private static final int REFSIZE = 5;
  /** Size of a block reference in bits. */
  private static final int REFSIZEBITS = REFSIZE * Byte.SIZE;
  /** Number of data blocks per header. */
  static final int BLOCKS = (BLOCKSIZEBITS - REFSIZEBITS) /
      (REFSIZEBITS + IO.BLOCKPOWER);
  /** Invalid block reference. */
  private static final long NIL = 0L;

  /** Reference to data blocks. */
  private final RecordAccess dataBlocks;
  /** Block index in the list of headers. */
  private int num = -1;

  // data stored on disk
  /** Next header block; {@link #NIL} if the last one. */
  private long next = NIL;
  /** Id numbers of blocks managed by this header block. */
  final long[] blocks = new long[BLOCKS];
  /** Free space in each block. */
  final int[] used = new int[BLOCKS];

  public HeaderBlocks(final RecordAccess data) {
    super(data.da);
    dataBlocks = data;
    if(da.length() == 0) addr = da.createBlock();
  }

  /**
   * Constructor; open a file for data block access.
   * @param f file
   * @param data reference to data blocks
   * @throws IOException I/O exception
   */
  public HeaderBlocks(final File f, final RecordAccess data)
      throws IOException {
    super(f);
    dataBlocks = data;
  }

  /**
   * Find a data block with enough free space; if no existing data block has
   * enough space, then a new data block is allocated; if a data block cannot
   * be allocated in one of the existing header blocks, a new header block will
   * be allocated.
   * @param rsize record size
   * @return data block index which has enough space
   */
  public int findBlock(final int rsize) {
    // space needed is record + record length + slot
    final int size = rsize + Num.length(rsize) + 2;
    // max used space in a block in order to be able to store the record
    final int max = IO.BLOCKSIZE - size;

    // search existing header blocks for a data block with enough space
    long n = 0L;
    num = 0;
    do {
      gotoBlock(n);
      // check the data blocks of the header for enough space
      for(int i = 0; i < used.length; ++i) {
        if(used[i] <= max) {
          if(blocks[i] == NIL) {
            // the reference is empty: allocate new data block
            blocks[i] = dataBlocks.da.createBlock();
            used[i] = 3;
            dirty = true;
          }
          return i + num * BLOCKS;
        }
      }
      ++num;
      n = next;
    } while(n != NIL);

    // no header block has empty data blocks: allocate new a new header block
    next = da.createBlock();
    dirty = true;

    // allocate the first data block of the new header block
    gotoBlock(next);
    blocks[0] = dataBlocks.da.createBlock();
    used[0] = 3;
    dirty = true;

    return num * BLOCKS;
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

    // NEXT is stored first
    next = da.read5();

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < BLOCKS; ++i) blocks[i] = da.read5();

    // FREE are stored next
    int p = (BLOCKS + 1) * REFSIZE;
    final int n = BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      used[i] = da.readLow12Bits(p, ++p);
      used[i + 1] = da.readHigh12Bits(p, ++p);
    }
    used[n] = da.readLow12Bits(p, ++p);
  }

  @Override
  protected void writeMetaData() {
    da.gotoBlock(addr);

    // NEXT is stored first
    da.write5(next);

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < BLOCKS; ++i) da.write5(blocks[i]);

    // FREE are stored next
    int p = (BLOCKS + 1) * REFSIZE;
    final int n = BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      da.writeLow12Bits(p, ++p, used[i]);
      da.writeHigh12Bits(p, ++p, used[i + 1]);
    }
    da.writeLow12Bits(p, ++p, used[n]);
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
