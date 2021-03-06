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

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;

/**
 * Oracle has a non-standard way of creating array parameter for stored
 * procedure. Hence we created this interface
 *
 * @author simplity.org
 *
 */
@FunctionalInterface
public interface ArrayCreator {
	/**
	 *
	 * @param con
	 *            active connection to teh db
	 * @param data
	 *            array of objects in the right order for the truct
	 * @param arrayType
	 *            name of the struct entity defined in the db.
	 * @return a struct object based on data
	 * @throws SQLException
	 */
	public Array createArray(Connection con, Struct[] data, String arrayType) throws SQLException;
}
