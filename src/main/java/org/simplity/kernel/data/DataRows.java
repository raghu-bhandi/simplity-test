/*
 * Copyright (c) 2016 simplity.org
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
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.simplity.kernel.data;

import java.util.Iterator;

/**
 * object returned by DataSheetInterface for iteration
 *
 * @author simplity.org
 */
public class DataRows implements Iterator<FieldsCollection> {
  private final DataSheet dataSheet;
  private int nextIdx = 0;
  private final int endIdx;

  DataRows(DataSheet dataSheet) {
    this.dataSheet = dataSheet;
    this.endIdx = dataSheet.length();
  }

  @Override
  public boolean hasNext() {
    return this.nextIdx < this.endIdx;
  }

  @Override
  public FieldsCollection next() {
    if (this.nextIdx >= this.endIdx) {
      return null;
    }
    return new DataRow(this.dataSheet, this.nextIdx++);
  }

  @Override
  /** feature is not supported */
  public void remove() {
    // we don't even want to throw an error.
  }
}
