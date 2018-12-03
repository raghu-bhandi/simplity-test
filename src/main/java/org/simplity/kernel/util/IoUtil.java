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

package org.simplity.kernel.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author simplity.org
 *
 */
public class IoUtil {
	private static final Logger logger = LoggerFactory.getLogger(IoUtil.class);

	/**
	 * read input stream into a string
	 *
	 * @param reader
	 * @return content of reader as string
	 */
	public static String readerToText(Reader reader) {
		StringBuilder sbf = new StringBuilder();
		try {
			int ch;
			while ((ch = reader.read()) > -1) {
				sbf.append((char) ch);
			}
			return sbf.toString();
		} catch (Exception e) {
			logger.error("Error while reading from reader. {}", e.getMessage());
			return null;
		} finally {
			try {
				reader.close();
			} catch (Exception ignore) {
				//
			}
		}
	}

	/**
	 * read input stream into a string
	 *
	 * @param stream
	 * @return content of reader as string
	 */
	public static String streamToText(InputStream stream) {
		StringBuilder sbf = new StringBuilder();
		try {
			int ch;
			while ((ch = stream.read()) > -1) {
				sbf.append((char) ch);
			}
			return sbf.toString();
		} catch (Exception e) {
			logger.error("Error while reading from stream. {}", e.getMessage());
			return null;
		}
	}

	/**
	 * read a resource into text
	 *
	 * @param fileOrResourceName
	 * @return text content of the resource. null in case of any error
	 */
	public static String readResource(String fileOrResourceName) {
		try (InputStream stream = getStream(fileOrResourceName)) {
			if (stream != null) {
				return streamToText(stream);
			}
		} catch (Exception e) {
			logger.error("Exception while reading resource {} using. Error: {}", fileOrResourceName,
					e.getMessage());
		}
		return null;
	}

	/**
	 * creates a stream for the resource from file system or using class loader
	 * @param fileOrResourceName should be  valid file-path, like c:/a/b/c.xxx, or a resource path like /a/b/c.xxx
	 * @return stream, or null in case of any trouble creating one
	 */
	public static InputStream getStream(String fileOrResourceName) {
		if(fileOrResourceName.charAt(0) == '/') {
			return IoUtil.class.getClassLoader().getResourceAsStream(fileOrResourceName);
		}
		File file = new File(fileOrResourceName);
		if(file.exists()) {
			try {
			return new FileInputStream(file);
			}catch(Exception e) {
				logger.error("Resource {} is intepreted as a file that was located on the file system, but error while creating stream from that file. Error: {}", fileOrResourceName, e.getMessage());
			}
		}
		return null;
	}
}
