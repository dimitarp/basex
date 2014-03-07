package org.basex.io.random.file;

import org.basex.io.IO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class MemoryMappedBlockFileAccess extends BlockFileAccess {
  private final FileChannel file;
  private final MappedByteBuffer fileBuffer;
  private boolean modified;

  MemoryMappedBlockFileAccess(final FileChannel f) throws IOException {
    this.file = f;
    this.fileBuffer = f.map(FileChannel.MapMode.READ_WRITE, 0, f.size());
  }

  @Override
  public void close() throws IOException {
    if(modified) fileBuffer.force();
    file.close();
  }

  @Override
  public void write(Buffer b) throws IOException {
    modified = true;
    ByteBuffer buffer = b.getByteBuffer();
    buffer.rewind();
    fileBuffer.position((int) b.getPos());
    fileBuffer.put(b.getByteBuffer());
    buffer.rewind();
    b.setDirty(false);
  }

  @Override
  public void read(Buffer b, int max) throws IOException {
    read(b);
  }

  @Override
  public void read(Buffer b) throws IOException {
    fileBuffer.position((int) b.getPos());
    ByteBuffer block = fileBuffer.slice();
    block.limit(IO.BLOCKSIZE);
    b.setByteBuffer(block);
  }

  @Override
  public long length() throws IOException {
    return file.size();
  }

  @Override
  public void setLength(long l) throws IOException {
    file.truncate(l);
  }

  @Override
  public FileChannel getChannel() {
    return file;
  }
}
