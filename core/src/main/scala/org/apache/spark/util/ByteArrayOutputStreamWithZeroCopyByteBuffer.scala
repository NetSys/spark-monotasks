/*
 * Copyright 2014 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * A ByteArrayOutputStream that will convert the underlying byte array to a byte buffer without
 * copying all of the data. This is to avoid calling the ByteArrayOutputStream.toByteArray
 * method, because that method makes a copy of the byte array.
 */
private[spark] class ByteArrayOutputStreamWithZeroCopyByteBuffer extends ByteArrayOutputStream {

  def getByteBuffer(): ByteBuffer = ByteBuffer.wrap(buf, 0, size())
}
