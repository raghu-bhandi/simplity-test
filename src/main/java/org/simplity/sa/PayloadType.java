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

package org.simplity.sa;

/**
 * @author simplity.org
 *
 */
public enum PayloadType {
	/**
	 * json text
	 */
	JSON_TEXT(false, false),
	/**
	 * xml text
	 */
	XML_TEXT(false, false),
	/**
	 * json object
	 */
	JSON_OBJECT(true, false),
	/**
	 * xml object
	 */
	XML_OBJECT(false, false),
	/**
	 * json reader/writer
	 */
	JSON_STREAM(true, false),
	/**
	 * xml reader/writer
	 */
	XML_STREAM(false, true);

	private final boolean isJson;
	private final boolean isStream;

	PayloadType(boolean json, boolean stream) {
		this.isJson = json;
		this.isStream = stream;
	}
	/**
	 * is it a json?
	 *
	 * @return true if it is json. false otherwise
	 */
	public boolean isJson() {
		return this.isJson;
	}

	/**
	 * is this a stream?
	 *
	 * @return true if it is stream, false otherwise
	 */
	public boolean isStream() {
		return this.isStream;
	}
}
