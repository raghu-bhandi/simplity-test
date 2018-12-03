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
package org.simplity.adapter.target;

import java.util.Date;

/**
 * target that can receive data from an adapter
 *
 * @author simplity.org
 *
 */
public interface DataTarget {
	/**
	 * receive a primitive value
	 *
	 * @param fieldName
	 * @param fieldValue
	 */
	public void setPrimitiveValue(String fieldName, String fieldValue);

	/**
	 * receive a date value
	 *
	 * @param fieldName
	 * @param fieldValue
	 */
	public void setDateValue(String fieldName, Date fieldValue);

	/**
	 * receive an Object
	 *
	 * @param fieldName
	 * @param value
	 *
	 */
	public void setObjectValue(String fieldName, Object value);

	/**
	 *
	 * @param fieldName
	 *            non-null
	 * @param childClassName
	 *            can be null, or a valid class name that can be used to
	 *            instantiate an instance
	 * @return a target suitable to receive data from another object, or null if
	 *         such a target can not be created
	 */
	public DataTarget getChildTarget(String fieldName, String childClassName);

	/**
	 *
	 * @param fieldName
	 * @param cildClassName
	 *            optional. useful with targets that use Generic list instead of
	 *            Array
	 * @return list of data target, meant for a field that is a list.
	 */
	public DataListTarget getChildListTarget(String fieldName, String cildClassName);
}
