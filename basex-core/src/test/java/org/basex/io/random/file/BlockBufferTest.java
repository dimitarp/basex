package org.basex.io.random.file;

public class BlockBufferTest extends BufferTest {
  @Override
  protected Buffer getBuffer(int size) {
    return new BlockBuffer(size);
  }
}
