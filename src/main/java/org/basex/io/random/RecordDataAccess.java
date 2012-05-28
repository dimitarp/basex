package org.basex.io.random;

import static org.basex.io.IO.*;
import static org.basex.util.BlockAccessUtil.*;

import java.io.*;
import java.util.*;

import org.basex.io.*;
import org.basex.util.*;

/**
 * Storage of variable-length records.
 * @author BaseX Team 2005-12, BSD License
 * @author Dimitar Popov
 *
 * <br/>
 * <p>A file contains two types of blocks: directory and data blocks. The records, are
 * stored in data blocks. Each record is identified by a record id with length 40 bits;
 * the high 28 bit identify the data block; the low 12 bits identify the record within the
 * data block.</p>
 * <br/>
 * <br/>
 *
 * <h1>DIRECTORY BLOCKS</h1>
 * <p>Directory blocks are organized in a linked list and contain references to the data
 * blocks and how much space is used in each data block. Each directory block has the
 * following structure:</p>
 * <ol>
 * <li>NEXT</li>
 * <p>Pointer to the next directory block in the list; size: 5 bytes.</p>
 * <li>BLOCKS</li>
 * <p>Pointers to the data blocks managed by the current directory block; number: 629;
 * size: 5 bytes.</p>
 * <li>USED</li>
 * <p>Number of used bytes in each data block managed by the current directory block;
 * number: 629; size: 12 bits.</p>
 * </ol>
 * <br/>
 * <br/>
 *
 * <h1>DATA BLOCKS</h1>
 * <p>Data blocks are divided into two areas: data and meta-data area. The data area is at
 * the beginning of the block. The meta-data area is at the end of the block.</p>
 * <ol>
 * <li>DATA AREA</li>
 * <p>The data area contains the records. A record is a contiguous sequence of bytes
 * starting with the record data length. The data area may be fragmented due to record
 * being deleted. In order to use the space of deleted records, the data area can be de-
 * fragmented.</p>
 * <li>META-DATA AREA</li>
 * <p>The meta data area has the following structure (the fields are stored in reverse
 * order starting from the end of the data block):</p>
 * <ol>
 * <li>SIZE</li>
 * <p>Size shows how many bytes does the data area occupy. More precisely, it shows the
 * last used byte + 1 used by the data area.</p>
 * <li>NUM</li>
 * <p>Number of slots, allocated in this data block.</p>
 * <li>SLOTS</li>
 * <p>Slots are offsets from the beginning of the data area, which show where a record is
 * stored. The index of a slot is used as the lowest 12 bits in a record id. If a record
 * is deleted, then the value stored in a slot is set to NIL. If a record, which occupies
 * the last slot, is deleted, then the number of slots is decremented.</p>
 * </ol>
 * </ol>
 */
public class RecordDataAccess {
  /** Size of a record identifier in bytes. */
  public static final int RIDSIZE = 5;
  /** Current data block. */
  private final DataBlock data;
  /** Directory. */
  private final Directory directory;

  /**
   * Constructor; open a file for data block access.
   * @param f file
   * @throws IOException I/O exception
   */
  public RecordDataAccess(final IOFile f) throws IOException {
    final BlockDataAccess da = new BlockDataAccess(f);
    directory = new Directory(da);
    data = new DataBlock(da);
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    data.flush();
    directory.flush();
  }

  /**
   * Close file.
   * @throws IOException I/O exception
   */
  public void close() throws IOException {
    flush();
    data.close();
    directory.close();
  }

  /**
   * Retrieve a record with the given id.
   * @param rid record id
   * @return record data
   */
  public byte[] select(final long rid) {
    data.gotoBlock(directory.lookupDataBlock(block(rid)));
    if(data.isCurrentBlockChunk()) return readChunked();
    return data.read(slot(rid));
  }

  /**
   * Retrieve a chunked record starting from the current data block.
   * @return record data
   */
  private byte[] readChunked() {
    final TokenBuilder builder = new TokenBuilder(2 * DataBlock.CHUNK_SIZE);
    final byte[] buf = new byte[DataBlock.CHUNK_SIZE];

    // read all blocks, which store chunks
    long next = 0L;
    while(data.isCurrentBlockChunk()) {
      next = data.readChunk(buf);
      builder.add(buf);
      data.gotoBlock(directory.lookupDataBlock(block(next)));
    }

    // the last chunk is stored as a normal record
    builder.add(data.read(slot(next)));

    return builder.finish();
  }

  /**
   * Delete record with the given id.
   * @param rid record id
   */
  public void delete(final long rid) {
    final int slot = slot(rid);
    final int blockNumber = block(rid);

    data.gotoBlock(directory.lookupDataBlock(blockNumber));

    if(data.isCurrentBlockChunk()) {
      deleteChunked(blockNumber);
    } else {
      final int deleted = data.delete(slot);
      if(deleted == IO.BLOCKSIZE) {
        directory.deleteDataBlock(blockNumber);
      } else {
        directory.updateDataBlockUsed(blockNumber, -deleted);
      }
    }
  }

  /**
   * Delete a chunked record starting from the current data block.
   * @param dataBlockNumber logical data block number
   */
  private void deleteChunked(final int dataBlockNumber) {
    long next = 0L;
    int blockNumber = dataBlockNumber;

    // delete all blocks, which store chunks
    while(data.isCurrentBlockChunk()) {
      next = data.deleteChunk();
      directory.deleteDataBlock(blockNumber);

      // goto next chunk
      blockNumber = block(next);
      data.gotoBlock(directory.lookupDataBlock(blockNumber));
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
    directory.setFindDataBlockStart(0);
    return append(record);
  }

  /**
   * Insert a record into the next possible area starting from the current
   * position (i.e. does not traverse the whole directory).
   * @param record record data
   * @return record id
   */
  public long append(final byte[] record) {
    return append(record, 0, record.length);
  }

  /**
   * Insert a record into the next possible area starting from the current position (i.e.
   * does not traverse the whole directory).
   * @param buf buffer, containing the record
   * @param offset offset in the buffer where the record starts
   * @param length record length
   * @return record id
   */
  public long append(final byte[] buf, final int offset, final int length) {
    if(length > DataBlock.MAX_SIZE) return appendChunked(buf, offset, length);

    // space needed is: record length + |record length| + |slot| + |SIZE| + |NUM|
    final int blockNumber = directory.findDataBlock(length + Num.length(length) + 2 + 3);

    long blockAddress = directory.lookupDataBlock(blockNumber);
    if(blockAddress == Directory.NIL) {
      // create a new block, if the current pointer is NIL
      blockAddress = data.createBlock();
      directory.insertDataBlock(blockNumber, blockAddress);
    }

    data.gotoBlock(blockAddress);
    final int slot = data.findEmptySlot();
    final int inserted = data.insert(slot, buf, offset, length);
    directory.updateDataBlockUsed(blockNumber, inserted);

    return rid(blockNumber, slot);
  }

  /**
   * Store a chunk. A chunk occupies a whole data block and has the record id of the next
   * chunk.
   * @param buf buffer with the record data
   * @param offset offset in the buffer where the record starts
   * @param rid record id of the next chunk
   * @return record id of the stored chunk
   */
  private long appendChunk(final byte[] buf, final int offset, final long rid) {

    final int blockNumber = directory.findDataBlock(DataBlock.CHUNK_SIZE);

    long blockAddress = directory.lookupDataBlock(blockNumber);
    if(blockAddress == Directory.NIL) {
      // create a new block, if the current pointer is NIL
      blockAddress = data.createBlock();
      directory.insertDataBlock(blockNumber, blockAddress);
    }

    data.writeChunk(blockAddress, buf, offset, rid);
    directory.setChunkDataBlock(blockNumber);

    return rid(blockNumber, 0);
  }

  /**
   * Split a record into chunks and store the chunks in separate blocks as a linked list.
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
   * Extract the logical block number from a record id.
   * @param rid record id
   * @return logical block number
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

/** Common fields for both directory and data blocks. */
abstract class Block {
  /** Underlying data file. */
  protected final BlockDataAccess da;
  /** Dirty flag. */
  protected boolean dirty;
  /** Address of the current block in the underlying storage. */
  protected long addr = -1;

  /** Write the directory block data to the underlying storage. */
  protected abstract void writeMetaData();

  /**
   * Constructor.
   * @param data underlying data storage
   */
  protected Block(final BlockDataAccess data) {
    da = data;
  }

  /**
   * Flush cached data to the disk.
   * @throws IOException I/O exception
   */
  final void flush() throws IOException {
    if(dirty) writeMetaData();
    da.flush();
  }

  /**
   * Flush cached data to the disk and close file handles.
   * @throws IOException I/O exception
   */
  final void close() throws IOException {
    da.close();
  }
}

/** Directory. */
final class Directory extends Block {
  /** Size of a block reference in bytes. */
  private static final int REFSIZE = 5;
  /** Size of a block reference in bits. */
  private static final int REFSIZEBITS = REFSIZE * 8;
  /**
   * Number of data blocks per directory block; a ref (40 bits) and number of used bytes
   * (12 bits) are stored. The first ref in the block is the next directory block.
   */
  private static final int BLOCKS = (BLOCKSIZE * 8 - REFSIZEBITS)
      / (REFSIZEBITS + BLOCKPOWER);
  /** Invalid block reference. */
  static final long NIL = 0L;

  /** Block index in the directory. */
  private int num = -1;
  /** Data block last used for insertion. */
  private int last;

  // data stored on disk
  /** Next directory block; {@link #NIL} if the last one. */
  private long next = NIL;
  /**
   * Used space in each block. The actual used space is +3 bytes, since the last 3 bytes
   * of each data block are occupied.
   */
  private final int[] used = new int[BLOCKS];
  /** Id numbers of blocks managed by the current directory block. */
  private final long[] blocks = new long[BLOCKS];

  /**
   * Constructor.
   * @param data underlying data storage.
   */
  Directory(final BlockDataAccess data) {
    super(data);
    if(da.length() == 0) {
      addr = da.createBlock();
      num = 0;
    }
  }

  /**
   * Mark that the given data block has a chunk. A data block which has a chunk has SIZE
   * = {@link DataBlock#EMPTYSLOT}.
   * @param blockNumber logical data block number
   */
  void setChunkDataBlock(final int blockNumber) {
    final int blockIndex = gotoDataBlock(blockNumber);
    used[blockIndex] = DataBlock.EMPTYSLOT;
    last = blockIndex;
    dirty = true;
  }

  /**
   * Get the address of a data block with a given logical number.
   * @param blockNumber logical data block number
   * @return data block address
   */
  long lookupDataBlock(final int blockNumber) {
    final int blockIndex = gotoDataBlock(blockNumber);
    return blocks[blockIndex];
  }

  /**
   * Add the data block with the given address and logical number.
   * @param blockNumber logical data block number
   * @param blockAddress physical data block address
   */
  void insertDataBlock(final int blockNumber, final long blockAddress) {
    final int blockIndex = gotoDataBlock(blockNumber);
    blocks[blockIndex] = blockAddress;
    used[blockIndex] = 0;
    dirty = true;
  }

  /**
   * Remove the data block with the given logical number.
   * @param blockNumber logical data block number
   */
  void deleteDataBlock(final int blockNumber) {
    insertDataBlock(blockNumber, Directory.NIL);
  }

  /**
   * Update the number of used bytes for a data block.
   * @param blockNumber logical data block number
   * @param increment number of used bytes to add (can be negative)
   */
  void updateDataBlockUsed(final int blockNumber, final int increment) {
    final int blockIndex = gotoDataBlock(blockNumber);
    used[blockIndex] += increment;
    dirty = true;
  }

  /**
   * Find a data block with enough free space; if no existing data block has enough space,
   * then a new data block is allocated; if a data block cannot be allocated in one of the
   * existing directory blocks, a new directory block will be allocated.
   * @param size free space needed
   * @return logical data block number which has enough space
   */
  int findDataBlock(final int size) {
    // max used space in a block in order to be able to store the record
    final int max = BLOCKSIZE - size;

    // search existing directory blocks for a data block with enough space
    long directoryBlockAddr = addr;
    do {
      gotoBlock(directoryBlockAddr);
      // check the data blocks of the directory block for enough space
      for(int i = last; i < used.length; ++i) {
        if(used[i] <= max) {
          last = i;
          return last + num * BLOCKS;
        }
      }
      ++num;
      directoryBlockAddr = next;
    } while(directoryBlockAddr != Directory.NIL);

    // no directory block has empty data blocks: allocate a new directory block
    next = da.createBlock();
    dirty = true;

    gotoBlock(next);
    init();

    return num * BLOCKS;
  }

  /**
   * Set the index of the directory block from which to start search for free space.
   * @param start index of first directory block to check
   */
  void setFindDataBlockStart(final int start) {
    gotoDirectoryBlock(start);
    last = 0;
  }

  /** Initialize the current directory block. */
  private void init() {
    last = 0;
    next = NIL;
    Arrays.fill(blocks, NIL);
    Arrays.fill(used, 0);
  }

  /**
   * Go to the directory block containing the reference to the data block with the given
   * logical block number and return the data block index.
   * @param blockNumber logical data block number
   * @return data block index in the directory block
   */
  private int gotoDataBlock(final int blockNumber) {
    gotoDirectoryBlock(blockNumber / BLOCKS);
    return blockNumber % BLOCKS;
  }

  /**
   * Go to a directory block.
   * @param n directory block index
   */
  private void gotoDirectoryBlock(final int n) {
    if(num == n) return;
    // start from the beginning, if n is smaller than the current directory block index
    if(num > n) {
      gotoBlock(0L);
      num = 0;
    }
    // scan directory, until the required index is reached
    // TODO
    //for(; num < n && next != Directory.NIL; ++num) gotoBlock(next);
    for(; num < n; ++num) gotoBlock(next);
  }

  /**
   * Go to the directory block at the specified address.
   * @param blockAddr block address
   */
  private void gotoBlock(final long blockAddr) {
    if(addr == blockAddr) return;
    if(dirty) writeMetaData();
    addr = blockAddr;
    read();
  }

  /** Read the directory block data from the underlying storage. */
  private void read() {
    da.gotoBlock(addr);
    last = 0;

    // NEXT is stored first
    next = da.read5();

    // BLOCK ADDRESSES are stored next
    for(int i = 0; i < Directory.BLOCKS; ++i) blocks[i] = da.read5();

    // USED are stored next
    int p = (Directory.BLOCKS + 1) * Directory.REFSIZE;
    final int n = Directory.BLOCKS - 1;
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
    for(int i = 0; i < Directory.BLOCKS; ++i) da.write5(blocks[i]);

    // USED are stored next
    int p = (Directory.BLOCKS + 1) * Directory.REFSIZE;
    final int n = Directory.BLOCKS - 1;
    for(int i = 0; i < n; i += 2, ++p) {
      da.writeLow12Bits(p, ++p, used[i]);
      da.writeHigh12Bits(p, ++p, used[i + 1]);
    }
    da.writeLow12Bits(p, ++p, used[n]);
    dirty = false;
  }
}

/** Data block. */
final class DataBlock extends Block {
  /** Bit mask used to extract the slot number from a record id. */
  static final long SLOT_MASK = BLOCKSIZE - 1L;
  /** Empty slot marker. */
  static final int EMPTYSLOT = (int) SLOT_MASK;
  /** Maximal length of a record (|SIZE + NUM + slot| = 5). */
  static final int MAX_SIZE = BLOCKSIZE - Num.length(BLOCKSIZE) - 5;
  /** Size of a chunk (|SIZE + NUM| = 3). */
  static final int CHUNK_SIZE = BLOCKSIZE - RecordDataAccess.RIDSIZE - 3;
  /** Size of an offset within a block in bits. */
  private static final int OFFSET_SIZE = BLOCKPOWER;
  /** Maximal number of records in a data block. */
  private static final int MAX_RECORDS = BLOCKSIZE / 2;

  /** Size of the meta data of the current block in bits. */
  private int metaDataSize;

  // fields stored in the block:
  /** Data area size (in bytes). */
  private int currentBlockSize;
  /** Number of slots. */
  private int currentBlockSlotCount;
  /** Slots with record offsets in the data area. */
  private final int[] currentBlockSlots = new int[MAX_RECORDS];

  /**
   * Constructor.
   * @param data underlying data storage
   */
  DataBlock(final BlockDataAccess data) {
    super(data);
  }

  /**
   * Read a chunk in the specified buffer and return the rid of the next chunk.
   * @param buf buffer to fill with the chunk data
   * @return rid of the next chunk
   */
  long readChunk(final byte[] buf) {
    da.readBytes(buf);
    return da.read5();
  }

  /**
   * Delete the current block and return the rid of the next chunk.
   * @return next chunk rid
   */
  long deleteChunk() {
    da.off = DataBlock.CHUNK_SIZE;
    final long next = da.read5();
    deleteBlock();
    return next;
  }

  /**
   * Create a new block and go to it.
   * @return address of the new block
   */
  long createBlock() {
    if(dirty) writeMetaData();

    currentBlockSize = 0;
    currentBlockSlotCount = 0;
    metaDataSize = 3;
    dirty = true;
    return addr = da.createBlock();
  }

  /** Delete the current block. */
  private void deleteBlock() {
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
    final int off = da.off = currentBlockSlots[slot];
    int len = da.readNum();
    len += Num.length(len);

    // decrease SIZE if the record is at the end of the DATA AREA
    if(off + len == currentBlockSize) currentBlockSize -= len;

    // mark slot as empty
    currentBlockSlots[slot] = EMPTYSLOT;

    // clean up empty slots
    for(; currentBlockSlotCount > 0; --currentBlockSlotCount)
      if(currentBlockSlots[currentBlockSlotCount - 1] != EMPTYSLOT) break;

    if(currentBlockSlotCount == 0) {
      deleteBlock();
      return IO.BLOCKSIZE;
    }

    final int newMetaDataSize = (currentBlockSlotCount + 2) * OFFSET_SIZE;
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
    currentBlockSize += len;
    if(slot == currentBlockSlotCount) {
      // new slot will be allocated
      ++currentBlockSlotCount;
      // increase the size of the meta data
      metaDataSize += DataBlock.OFFSET_SIZE;
      // if slot is even, then 1 new byte will be allocated; else 2 bytes
      len += (currentBlockSlotCount & 1) == 0 ? 1 : 2;
    }

    currentBlockSlots[slot] = off;
    dirty = true;

    return len;
  }

  /**
   * Read the record for the given slot of the current block.
   * @param slot slot index
   * @return record data
   */
  byte[] read(final int slot) {
    da.off = currentBlockSlots[slot];
    return da.readToken();
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
    currentBlockSize = DataBlock.EMPTYSLOT;
    currentBlockSlotCount = 0;
    dirty = true;
  }

  /**
   * Check if the current block contains a record chunk.
   * @return {@code true} if the current block contains a record chunk
   */
  boolean isCurrentBlockChunk() {
    return currentBlockSize == DataBlock.EMPTYSLOT;
  }

  /** Read the meta-data of a data block. */
  private void readMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    currentBlockSize = da.readLow12Bits(BLOCKSIZE - 1, BLOCKSIZE - 2);
    // NUMBER OF SLOTS is stored the next 12 bits
    currentBlockSlotCount = da.readHigh12Bits(BLOCKSIZE - 2, BLOCKSIZE - 3);

    // SLOTS are stored next
    int p = BLOCKSIZE - 4;
    final boolean odd = (currentBlockSlotCount & 1) == 1;
    final int n = odd ? currentBlockSlotCount - 1 : currentBlockSlotCount;
    for(int i = 0; i < n; i += 2, --p) {
      currentBlockSlots[i] = da.readLow12Bits(p, --p);
      currentBlockSlots[i + 1] = da.readHigh12Bits(p, --p);
    }
    if(odd) currentBlockSlots[n] = da.readLow12Bits(p, --p);

    metaDataSize = (currentBlockSlotCount + 2) * DataBlock.OFFSET_SIZE;
  }

  @Override
  protected void writeMetaData() {
    da.gotoBlock(addr);

    // DATA AREA SIZE is stored at 12 bits the end of each block
    da.writeLow12Bits(BLOCKSIZE - 1, BLOCKSIZE - 2, currentBlockSize);
    // NUMBER OF SLOTS is stored the next 12 bits
    da.writeHigh12Bits(BLOCKSIZE - 2, BLOCKSIZE - 3, currentBlockSlotCount);

    // SLOTS are stored next
    int p = BLOCKSIZE - 4;
    final boolean odd = (currentBlockSlotCount & 1) == 1;
    final int n = odd ? currentBlockSlotCount - 1 : currentBlockSlotCount;
    for(int i = 0; i < n; i += 2, --p) {
      da.writeLow12Bits(p, --p, currentBlockSlots[i]);
      da.writeHigh12Bits(p, --p, currentBlockSlots[i + 1]);
    }
    if(odd) da.writeLow12Bits(p, --p, currentBlockSlots[n]);
    dirty = false;
  }

  /**
   * Find an empty slot; if none is available, create a new one at the end.
   * @return index of the empty slot
   */
  int findEmptySlot() {
    for(int r = 0; r < currentBlockSlotCount; ++r)
      if(currentBlockSlots[r] == DataBlock.EMPTYSLOT) return r;
    return currentBlockSlotCount;
  }

  /**
   * Allocate a given number of bytes from the free space area.
   * @param l number of bytes
   * @return offset within the block where the bytes have been allocated
   */
  private int allocate(final int l) {
    // re-organize records, if not enough space
    if(l > BLOCKSIZE - currentBlockSize - (int) divRoundUp(metaDataSize, 8)) compact();
    return currentBlockSize;
  }

  /** Compact records in a contiguous area. */
  private void compact() {
    // order the slots by offsets they have
    final int[] idx = createOrder(currentBlockSlotCount, currentBlockSlots);

    int ins = da.off = 0;
    for(int i = 0; i < currentBlockSlotCount; ++i) {
      final int off = currentBlockSlots[idx[i]];
      if(off == EMPTYSLOT) break;

      // read the record from the old position
      da.off = off;
      final byte[] record = da.readToken();
      if(ins < off) {
        // set the new position and write the record
        currentBlockSlots[idx[i]] = da.off = ins;
        da.writeToken(record);
      }
      // set the next insert position at the end of the record
      ins = da.off;
    }
    currentBlockSize = ins;
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
