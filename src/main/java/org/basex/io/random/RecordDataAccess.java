package org.basex.io.random;

import static org.basex.io.IO.*;
import static org.basex.util.BlockAccessUtil.*;

import java.io.*;
import java.util.*;

import org.basex.io.*;
import org.basex.util.*;

/**
 * Storage of variable-length records.
 * TODO: decide how records with size > 4096 will be stored
 * @author BaseX Team 2005-12, BSD License
 * @author Dimitar Popov
 *
 * <br/>
 * <p>A file contains two types of blocks: header and data blocks. The records,
 * are stored in data blocks. Each record is identified by a record id with
 * length 40 bits; the highest 28 bit identify the data block; the lowest 12
 * bits identify the record within the data block.</p>
 *
 * <h1>HEADER BLOCKS</h1>
 * Header blocks are organized in a linked list and contain references to the
 * data blocks and how much space is used in each data block. Each header block
 * has the following structure:
 * <ol>
 * <li>NEXT</li>
 * <p>Pointer to the next header block in the list; size: 5 bytes.</p>
 * <li>BLOCKS</li>
 * <p>Pointers to the data blocks managed by the current header block; number:
 * 629; size: 5 bytes.</p>
 * <li>USED</li>
 * <p>Number of used bytes in each data block managed by the current header
 * block; number: 629; size: 12 bits.</p>
 * </ol>
 *
 * <h1>DATA BLOCKS</h1>
 * <p>Data blocks are divided into two areas: data and meta-data area. The data
 * area is at the beginning of the block. The meta-data area is at the
 * end of the block.</p>
 * <ol>
 * <li>DATA AREA</li>
 * <p>The data area contains the records. A record is a contiguous sequence of
 * bytes starting with the record data length. The data area may be fragemented
 * due to records being deleted. In order to use the space of deleted records,
 * the data area can be de-fragmented.</p>
 * <li>META-DATA AREA</li>
 * <p>The meta data area has the following structure (the fields are stored in
 * reverse order starting from the end of the data block):</p>
 * <ol>
 * <li>SIZE</li>
 * <p>Size shows how many bytes does the data area occupy. More precisely, it
 * shows the last used byte + 1 used by the data area.</p>
 * <li>NUM</li>
 * <p>Number of slots, allocated in this data block.</p>
 * <li>SLOTS</li>
 * <p>Slots are offsets from the beginning of the data area, which show where a
 * record is stored. The index of a slot is used as the lowest 12 bits in a
 * record id. If a record is deleted, then the value stored in a slot is set to
 * NIL. If a record, which occupies the last slot, is deleted, then the number
 * of slots is decremented.</p>
 * </ol>
 * </ol>
 */
public class RecordDataAccess {
  /** Size of a record identifier in bytes. */
  public static final int RID_SIZE = 5;
  /** Current data block. */
  private final DataBlock block = new DataBlock();
  /** Current header block. */
  private final HeaderBlock header = new HeaderBlock();

  /**
   * Constructor; open a file for data block access.
   * @param f file
   * @throws IOException I/O exception
   */
  public RecordDataAccess(final IOFile f) throws IOException {
    header.da = block.da = new BlockDataAccess(f);
    if(header.da.length() == 0) {
      header.addr = header.da.createBlock();
      header.num = 0;
    }
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    if(block.dirty) block.writeMetaData();
    if(header.dirty) header.write();
    block.da.flush();
    header.da.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    flush();
    block.da.close();
    header.da.close();
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    block.gotoBlock(header.lookup(block(rid)));

    if(block.size == DataBlock.NIL) return readChunked();

    block.da.off = block.slots[slot(rid)];
    return block.da.readToken();
  }

  /**
   * Retrieve a chunked record starting from the current data block.
   * @return record data
   */
  private byte[] readChunked() {
    final TokenBuilder builder = new TokenBuilder(DataBlock.CHUNK_SIZE);
    final byte[] buf = new byte[DataBlock.CHUNK_SIZE];

    // read all blocks, which store chunks
    long next = 0L;
    while(block.size == DataBlock.NIL) {
      block.da.readBytes(buf);
      builder.add(buf);
      next = block.da.read5();
      block.gotoBlock(header.lookup(block(next)));
    }

    // the last chunk is stored as a normal record
    block.da.off = block.slots[slot(next)];
    builder.add(block.da.readToken());

    return builder.finish();
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int slot = slot(rid);
    final int blockNum = block(rid);
    final int blockIndex = blockNum % HeaderBlock.BLOCKS;
    block.gotoBlock(header.lookup(blockNum));

    if(block.size == DataBlock.NIL) {
      deleteChunked(blockIndex);
      return;
    }

    final int deleted = block.delete(slot);

    if(deleted == IO.BLOCKSIZE) {
      // all records are deleted: delete the whole block
      block.delete();
      header.delete(blockIndex);
    } else {
      header.used[blockIndex] -= deleted;
      header.dirty = true;
    }
  }

  /**
   * Delete a chunked record starting from the current data block.
   * @param dataBlockIndex index of the current data block in the current header
   */
  private void deleteChunked(final int dataBlockIndex) {
    long next = 0L;
    int blockIndex = dataBlockIndex;

    // delete all blocks, which store chunks
    while(block.size == DataBlock.NIL) {
      block.da.off = DataBlock.CHUNK_SIZE;
      next = block.da.read5();

      block.delete();
      header.delete(blockIndex);

      // goto next chunk
      final int blockNum = block(next);
      blockIndex = blockNum % HeaderBlock.BLOCKS;
      block.gotoBlock(header.lookup(blockNum));
    }

    // the last chunk is stored as a normal record
    delete(next);
  }

  /**
   * Insert a record.
   * @param record record data
   * @return record id
   */
  public long insert(final byte[] record) {
    header.gotoHeader(0);
    header.last = 0;
    return append(record);
  }

  /**
   * Insert a record into the next possible area starting from the current
   * position (i.e. does not traverse all headers).
   * @param record record data
   * @return record id
   */
  public long append(final byte[] record) {
    return append(record, 0, record.length);
  }

  /**
   * Insert a record into the next possible area starting from the current
   * position (i.e. does not traverse all headers).
   * @param buf buffer, containing the record
   * @param offset offset in the buffer where the record starts
   * @param length record length
   * @return record id
   */
  public long append(final byte[] buf, final int offset, final int length) {
    if(length > DataBlock.MAX_SIZE) return appendChunked(buf, offset, length);

    // space needed is record length + |record length| + |slot| + |SIZE| + |NUM|
    final int blockIndex = header.findBlock(length + Num.length(length) + 2 + 3);

    // create a new block, if the current pointer is NIL
    if(header.blocks[blockIndex] == HeaderBlock.NIL)
      header.blocks[blockIndex] = block.create();

    final int blockNum = blockIndex + header.num * HeaderBlock.BLOCKS;

    block.gotoBlock(header.blocks[blockIndex]);
    final int slot = block.findEmptySlot();

    header.used[blockIndex] += block.insert(slot, buf, offset, length);
    header.dirty = true;

    return rid(blockNum, slot);
  }

  /**
   * Split a record into chunks and store the chunks in separate blocks as a
   * linked list.
   * The rid of the next chunk is stored at the end of the previous chunk.
   * The last chunk is stored as a normal record.
   * The rid of the first chunk is the rid of the whole record.
   * @param buf buffer with the record data
   * @param offset offset in the buffer where the record starts
   * @param length record length
   * @return record id
   */
  private long appendChunked(final byte[] buf, final int offset, final int length) {

    final int last = length % DataBlock.CHUNK_SIZE;
    int p = offset + length - last;

    // chunks need to be stored in reverse order, so that the rid's are known
    long rid = append(buf, p, last);

    for(p -= DataBlock.CHUNK_SIZE; p >= offset; p -= DataBlock.CHUNK_SIZE) {
      rid = appendChunk(buf, p, rid);
    }

    return rid;
  }

  /**
   * Store a chunk. A chunk occupies a whole data block and has the record id of
   * the next chunk. A data block which has a chunk has
   * SIZE = {@link DataBlock#NIL} and NUM = 0.
   * @param buf buffer with the record data
   * @param offset offset in the buffer where the record starts
   * @param rid record id of the next chunk
   * @return record id of the stored chunk
   */
  private long appendChunk(final byte[] buf, final int offset, final long rid) {

    final int blockIndex = header.findBlock(DataBlock.CHUNK_SIZE);

    // create a new block, if the current pointer is NIL
    if(header.blocks[blockIndex] == HeaderBlock.NIL)
      header.blocks[blockIndex] = block.create();

    final int blockNum = blockIndex + header.num * HeaderBlock.BLOCKS;

    block.writeChunk(header.blocks[blockIndex], buf, offset, rid);

    // update the meta-data of the block
    header.used[blockIndex] = DataBlock.NIL;
    header.last = blockIndex;
    header.dirty = true;

    return rid(blockNum, 0);
  }

  /**
   * Extract the block index from a record id.
   * @param rid record id
   * @return block index
   */
  private static int block(final long rid) {
    return (int) (rid >>> BLOCKPOWER);
  }

  /**
   * Extract the slot index from a record id.
   * @param rid record id
   * @return slot index
   */
  private static int slot(final long rid) {
    return (int) (rid & DataBlock.SLOT_MASK);
  }

  /**
   * Construct a record identifier from block number and slot.
   * @param block block number
   * @param slot slot
   * @return record identifier
   */
  private static long rid(final int block, final int slot) {
    return (((long) block) << BLOCKPOWER) | slot;
  }
}

/** Common fields for both header and data blocks. */
abstract class Block {
  /** Underlying data file. */
  protected BlockDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;
}

/** Header block. */
class HeaderBlock extends Block {
  /** Size of a block reference in bytes. */
  static final int REF_SIZE = 5;
  /** Size of a block reference in bits. */
  static final int REF_SIZE_BITS = REF_SIZE << 3;
  /**
   * Number of data blocks per header; a ref (40 bits) and number of used bytes
   * (12 bits) are stored. The first ref in the block is the next header.
   */
  static final int BLOCKS = ((BLOCKSIZE << 3) - REF_SIZE_BITS) /
      (REF_SIZE_BITS + BLOCKPOWER);
  /** Invalid block reference. */
  static final long NIL = 0L;

  /** Block index in the list of headers. */
  int num = -1;
  /** Data block last used for insertion. */
  int last;

  // data stored on disk
  /** Next header block; {@link #NIL} if the last one. */
  long next = NIL;
  /** Id numbers of blocks managed by this header block. */
  final long[] blocks = new long[BLOCKS];
  /**
   * Used space in each block. The actual used space is +3 bytes, since the last
   * 3 bytes of each data block are occupied.
   */
  final int[] used = new int[BLOCKS];

  /** Initialize the current header. */
  void init() {
    last = 0;
    next = NIL;
    Arrays.fill(blocks, NIL);
    Arrays.fill(used, 0);
  }

  /**
   * Get the address of a data block with a given index.
   * @param blockIndex data block index
   * @return block address
   */
  long lookup(final int blockIndex) {
    gotoHeader(blockIndex / BLOCKS);
    return blocks[blockIndex % BLOCKS];
  }

  /**
   * Go to a header block.
   * @param n header block index
   */
  void gotoHeader(final int n) {
    if(num == n) return;
    // start from the beginning, if n is smaller than the current header index
    if(num > n) {
      gotoBlock(0L);
      num = 0;
    }
    // scan headers, until the required index is reached
    // TODO
    //for(; num < n && next != HeaderBlock.NIL; ++num) gotoBlock(next);
    for(; num < n; ++num) gotoBlock(next);
  }

  /**
   * Go to the header block at the specified address.
   * @param blockAddr block address
   */
  private void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) return;
    if(dirty) write();
    addr = blockAddr;
    read();
  }

  /**
   * Remove the data block with the given index from the header.
   * @param blockIndex data block index in the header
   */
  void delete(final int blockIndex) {
    blocks[blockIndex] = HeaderBlock.NIL;
    used[blockIndex] = 0;
    dirty = true;
  }

  /**
   * Find a data block with enough free space; if no existing data block has
   * enough space, then a new data block is allocated; if a data block cannot
   * be allocated in one of the existing header blocks, a new header block will
   * be allocated.
   * @param size free space needed
   * @return data block index which has enough space
   */
  int findBlock(final int size) {
    // max used space in a block in order to be able to store the record
    final int max = BLOCKSIZE - size;

    // search existing header blocks for a data block with enough space
    long headerAddr = addr;
    do {
      gotoBlock(headerAddr);
      // check the data blocks of the header for enough space
      for(int i = last; i < used.length; ++i) {
        if(used[i] <= max) {
          last = i;
          return last;
        }
      }
      ++num;
      headerAddr = next;
    } while(headerAddr != HeaderBlock.NIL);

    // no header block has empty data blocks: allocate a new header block
    next = da.createBlock();
    dirty = true;

    gotoBlock(next);
    init();

    last = 0;
    return last;
  }

  /** Read the header data from the underlying storage. */
  void read() {
    da.gotoBlock(addr);
    last = 0;

    // NEXT is stored first
    next = da.read5();

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i) blocks[i] = da.read5();

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REF_SIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      used[i] = da.readLow12Bits(p, ++p);
      used[i + 1] = da.readHigh12Bits(p, ++p);
    }
    used[n] = da.readLow12Bits(p, ++p);
  }

  /** Write the header data to the underlying storage. */
  void write() {
    da.gotoBlock(addr);

    // NEXT is stored first
    da.write5(next);

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < HeaderBlock.BLOCKS; ++i) da.write5(blocks[i]);

    // USED are stored next
    int p = (HeaderBlock.BLOCKS + 1) * HeaderBlock.REF_SIZE;
    final int n = HeaderBlock.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      da.writeLow12Bits(p, ++p, used[i]);
      da.writeHigh12Bits(p, ++p, used[i + 1]);
    }
    da.writeLow12Bits(p, ++p, used[n]);
    dirty = false;
  }
}

/** Data block. */
class DataBlock extends Block {
  /** Bit mask used to extract the slot number from a record id. */
  static final long SLOT_MASK = (1L << BLOCKPOWER) - 1L;
  /** Size of an offset within a block in bits. */
  static final int OFFSET_SIZE = BLOCKPOWER;
  /** Maximal number of records in a data block. */
  private static final int MAX_RECORDS = BLOCKSIZE >>> 1;
  /** Maximal length of a record (|SIZE + NUM + slot| = 5). */
  static final int MAX_SIZE = BLOCKSIZE - Num.length(BLOCKSIZE) - 5;
  /** Size of a chunk (|SIZE + NUM| = 3). */
  static final int CHUNK_SIZE = BLOCKSIZE - RecordDataAccess.RID_SIZE - 3;
  /** Empty slot marker. */
  static final int NIL = (int) SLOT_MASK;

  /** Size of the meta data of the current block in bits. */
  int metaDataSize;

  // fields stored in the block:
  /** Data area size (in bytes). */
  int size;
  /** Number of slots. */
  int num;
  /** Slots with record offsets in the data area. */
  final int[] slots = new int[MAX_RECORDS];

  /**
   * Create a new block and go to it.
   * @return address of the new block
   */
  long create() {
    if(dirty) writeMetaData();

    size = 0;
    num = 0;
    metaDataSize = 3;
    dirty = true;
    return addr = da.createBlock();
  }

  /** Delete the current block. */
  void delete() {
    da.deleteBlock(addr);
    addr = -1;
    dirty = false;
  }

  /**
   * Delete a record from a given slot.
   * @param slot record slot
   * @return number of deleted bytes or {@link IO#BLOCKSIZE} if block is empty
   */
  int delete(final int slot) {
    // read the record length
    final int off = da.off = slots[slot];
    int len = da.readNum();
    len += Num.length(len);

    // decrease SIZE if the record is at the end of the DATA AREA
    if(off + len == size) size -= len;

    // mark slot as empty
    slots[slot] = NIL;

    // clean up empty slots
    for(; num > 0; --num) if(slots[num - 1] != NIL) break;

    if(num == 0) return IO.BLOCKSIZE;

    final int newMetaDataSize = (num + 2) * OFFSET_SIZE;
    final int diff = len + ((metaDataSize - newMetaDataSize) >> 3);
    metaDataSize = newMetaDataSize;
    dirty = true;

    return diff;
  }

  /**
   * Insert a record at the given slot.
   * @param slot record slot
   * @param buf buffer with record data
   * @param offset offset in the buffer where the record starts
   * @param length length of the record
   * @return number of bytes used to store the record
   */
  int insert(final int slot, final byte[] buf, final int offset, final int length) {
    int len = Num.length(length) + length;

    // write the record data
    final int off = da.off = allocate(len);
    da.writeToken(buf, offset, length);

    // increase size
    size += len;
    if(slot == num) {
      // new slot will be allocated
      ++num;
      // increase the size of the meta data
      metaDataSize += DataBlock.OFFSET_SIZE;
      // if slot is even, then 1 new byte will be allocated; else 2 bytes
      len += (num & 1) == 0 ? 1 : 2;
    }

    slots[slot] = off;
    dirty = true;

    return len;
  }

  /**
   * Go to the data block at the specified address.
   * @param blockAddr block address
   */
  void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) {
      if(da.blockPos() != blockAddr) da.gotoBlock(blockAddr);
      return;
    }

    if(dirty) writeMetaData();
    addr = blockAddr;
    readMetaData();
  }

  /**
   * Write a chunk to a block.
   * @param blockAddr block address
   * @param buf buffer
   * @param off offset in the buffer
   * @param next record id of the next chunk
   */
  void writeChunk(final long blockAddr, final byte[] buf, final int off,
      final long next) {

    gotoBlock(blockAddr);

    // assume the block is empty: write directly the chunk and the next rid
    da.writeBytes(buf, off, DataBlock.CHUNK_SIZE);
    da.write5(next);

    // data blocks with chunks are recognized as follows:
    size = DataBlock.NIL;
    num = 0;
    dirty = true;
  }

  /** Read the meta-data of a data block. */
  void readMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    size = da.readLow12Bits(BLOCKSIZE - 1, BLOCKSIZE - 2);
    // NUMBER OF SLOTS is stored the next 12 bits
    num = da.readHigh12Bits(BLOCKSIZE - 2, BLOCKSIZE - 3);

    // SLOTS are stored next
    int p = BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      slots[i] = da.readLow12Bits(p, --p);
      slots[i + 1] = da.readHigh12Bits(p, --p);
    }
    if(odd) slots[n] = da.readLow12Bits(p, --p);

    metaDataSize = (num + 2) * DataBlock.OFFSET_SIZE;
  }

  /** Write the meta-data of a data block. */
  void writeMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    da.writeLow12Bits(BLOCKSIZE - 1, BLOCKSIZE - 2, size);
    // NUMBER OF SLOTS is stored the next 12 bits
    da.writeHigh12Bits(BLOCKSIZE - 2, BLOCKSIZE - 3, num);

    // SLOTS are stored next
    int p = BLOCKSIZE - 4;
    final boolean odd = (num & 1) == 1;
    final int n = odd ? num - 1 : num;
    for(int i = 0; i < n; i += 2, --p) {
      da.writeLow12Bits(p, --p, slots[i]);
      da.writeHigh12Bits(p, --p, slots[i + 1]);
    }
    if(odd) da.writeLow12Bits(p, --p, slots[n]);
    dirty = false;
  }

  /**
   * Find an empty slot; if none is available, create a new one at the end.
   * @return index of the empty slot
   */
  int findEmptySlot() {
    for(int r = 0; r < num; ++r) if(slots[r] == DataBlock.NIL) return r;
    return num;
  }

  /**
   * Allocate a given number of bytes from the free space area.
   * @param l number of bytes
   * @return offset within the block where the bytes have been allocated
   */
  int allocate(final int l) {
    // re-organize records, if not enough space
    if(l > BLOCKSIZE - size - (int) divRoundUp(metaDataSize, 8)) compact();
    return size;
  }

  /** Compact records in a contiguous area. */
  private void compact() {
    // order the slots by offsets they have
    final int[] idx = createOrder(num, slots);

    int ins = da.off = 0;
    for(int i = 0; i < num; ++i) {
      final int off = slots[idx[i]];
      if(off == NIL) break;

      // read the record from the old position
      da.off = off;
      final byte[] record = da.readToken();
      if(ins < off) {
        // set the new position and write the record
        slots[idx[i]] = da.off = ins;
        da.writeToken(record);
      }
      // set the next insert position at the end of the record
      ins = da.off;
    }
    size = ins;
  }

  /**
   * Create a list of indexes sorted by the values stored in an array.
   * @param n number of values from the beginning of the array
   * @param array array with values
   * @return list of indexes of the array
   */
  private static int[] createOrder(final int n, final int[] array) {
    final int[] tmp = new int[n];
    System.arraycopy(array, 0, tmp, 0, n);
    return Array.createOrder(tmp, true);
  }
}
