/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.saptarshiguha.rhipe.hadoop;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;



public class RXLineReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  private int bufferLength = 0;
  private int bufferPosn = 0;

  private static final byte CR = '\r';
  private static final byte LF = '\n';

  public RXLineReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  public RXLineReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  public RXLineReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
  }

  public void close() throws IOException {
    in.close();
  }
  
  public int readLine(RXWritableText str, int maxLineLength,
                      int maxBytesToConsume) throws IOException {
    str.clear();
    int txtLength = 0; //tracks str.getLength(), as an optimization
    int newlineLength = 0; //length of terminating newline
    boolean prevCharCR = false; //true of prev char was CR
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn; //starting from where we left off the last time
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        if (prevCharCR)
          ++bytesConsumed; //account for CR from previous read
        bufferLength = in.read(buffer);
        if (bufferLength <= 0)
          break; // EOF
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
        if (buffer[bufferPosn] == LF) {
          newlineLength = (prevCharCR) ? 2 : 1;
          ++bufferPosn; // at next invocation proceed from following byte
          break;
        }
        if (prevCharCR) { //CR + notLF, we are at notLF
          newlineLength = 1;
          break;
        }
        prevCharCR = (buffer[bufferPosn] == CR);
      }
      int readLength = bufferPosn - startPosn;
      if (prevCharCR && newlineLength == 0)
        --readLength; //CR at the end of the buffer
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > (long)Integer.MAX_VALUE)
      throw new IOException("Too many bytes before newline: " + bytesConsumed);    
    return (int)bytesConsumed;
  }

  public int readLine(RXWritableText str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
}

  public int readLine(RXWritableText str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

}
