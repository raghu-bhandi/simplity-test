/*
 * Copyright (c) 2018 simplity.org
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.simplity.kernel.idb;

/**
 * interface that specifies the functionality for reading data from a data base.
 * It may look like a round-about way to do the operation. This interface is
 * designed this way to ensure that the handle's life-cycle is controlled by the
 * driver, and not by end-users of this API
 *
 * @author simplity.org
 *
 */
public interface IDbReader {
	/**
	 * read whatever you want to read from dd using the read-only handle
	 * supplied. Note that the handle is active ONLY during this method. It will
	 * be closed on return from this function. Implementations should ensure
	 * that the handle is discarded after return.
	 *
	 * @param dbHandle
	 */
	public void read(IReadOnlyHandle dbHandle);
}
