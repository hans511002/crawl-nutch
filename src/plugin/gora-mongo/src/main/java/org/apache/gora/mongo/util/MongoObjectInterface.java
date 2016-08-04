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

package org.apache.gora.mongo.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.gora.util.AvroUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.hbase.Convert;

/**
 * Contains utility methods for byte[] <-> field conversions.
 */
public class MongoObjectInterface {
	/**
	 * Threadlocals maintaining reusable binary decoders and encoders.
	 */
	public static final ThreadLocal<BinaryDecoder> decoders = new ThreadLocal<BinaryDecoder>();
	public static final ThreadLocal<BinaryEncoderWithStream> encoders = new ThreadLocal<BinaryEncoderWithStream>();

	/**
	 * A BinaryEncoder that exposes the outputstream so that it can be reset every time. (This is a workaround to reuse
	 * BinaryEncoder and the buffers, normally provided be EncoderFactory, but this class does not exist yet in the
	 * current Avro version).
	 */
	public static final class BinaryEncoderWithStream extends BinaryEncoder {
		BinaryEncoder en = null;
		OutputStream out = null;

		public BinaryEncoderWithStream(OutputStream out) {
			this.out = out;
			en = EncoderFactory.get().directBinaryEncoder(out, en);
		}

		protected OutputStream getOut() {
			return out;
		}

		@Override
		public int bytesBuffered() {
			return en.bytesBuffered();
		}

		@Override
		public void writeBoolean(boolean b) throws IOException {
			en.writeBoolean(b);
		}

		@Override
		public void writeDouble(double d) throws IOException {
			en.writeDouble(d);
		}

		@Override
		public void writeFixed(byte[] bytes, int start, int len) throws IOException {
			en.writeFixed(bytes, start, len);
		}

		@Override
		public void writeFloat(float f) throws IOException {
			en.writeFloat(f);
		}

		@Override
		public void writeInt(int n) throws IOException {
			en.writeInt(n);
		}

		@Override
		public void writeLong(long n) throws IOException {
			en.writeLong(n);
		}

		@Override
		public void flush() throws IOException {
			en.flush();
		}

		@Override
		protected void writeZero() throws IOException {
			out.write(0);
		}
	}

	/*
	 * Create a threadlocal map for the datum readers and writers, because they are not thread safe, at least not before
	 * Avro 1.4.0 (See AVRO-650). When they are thread safe, it is possible to maintain a single reader and writer pair
	 * for every schema, instead of one for every thread.
	 */

	public static final ThreadLocal<Map<String, SpecificDatumReader<?>>> readerMaps = new ThreadLocal<Map<String, SpecificDatumReader<?>>>() {
		protected Map<String, SpecificDatumReader<?>> initialValue() {
			return new HashMap<String, SpecificDatumReader<?>>();
		};
	};

	public static final ThreadLocal<Map<String, SpecificDatumWriter<?>>> writerMaps = new ThreadLocal<Map<String, SpecificDatumWriter<?>>>() {
		protected Map<String, SpecificDatumWriter<?>> initialValue() {
			return new HashMap<String, SpecificDatumWriter<?>>();
		};
	};

	@SuppressWarnings("rawtypes")
	public static Object fromObject(Schema schema, Object val) throws IOException {
		Type type = schema.getType();
		switch (type) {
		case ENUM:
			return AvroUtils.getEnumValue(schema, val.toString());
		case STRING:
			return new Utf8(val.toString());
		case BYTES:
			return ByteBuffer.wrap(val.toString().getBytes());
		case INT:
			return Convert.ToInt(val.toString());
		case LONG:
			return Convert.ToLong(val.toString());
		case FLOAT:
			return Convert.ToFloat(val.toString());
		case DOUBLE:
			return Convert.ToDouble(val.toString());
		case BOOLEAN:
			return Convert.ToBool(val.toString());
		case RECORD:
			Map<String, SpecificDatumReader<?>> readerMap = readerMaps.get();
			SpecificDatumReader<?> reader = readerMap.get(schema.getFullName());
			if (reader == null) {
				reader = new SpecificDatumReader(schema);
				readerMap.put(schema.getFullName(), reader);
			}
			// initialize a decoder, possibly reusing previous one
			BinaryDecoder decoderFromCache = decoders.get();
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(val.toString().getBytes(), decoderFromCache);
			// put in threadlocal cache if the initial get was empty
			if (decoderFromCache == null) {
				decoders.set(decoder);
			}
			return reader.read(null, decoder);
		default:
			throw new RuntimeException("Unknown type: " + type);
		}
	}

	@SuppressWarnings("unchecked")
	public static <K> K fromString(Class<K> clazz, String val) {
		if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
			return (K) Byte.valueOf(Bytes.toBytes(val)[0]);
		} else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
			return (K) Boolean.valueOf("true".equals(val));
		} else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
			return (K) Short.valueOf((short) Convert.ToInt(val));
		} else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
			return (K) Integer.valueOf(Convert.ToInt(val));
		} else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
			return (K) Long.valueOf(Convert.ToLong(val));
		} else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
			return (K) Float.valueOf(Convert.ToFloat(val));
		} else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
			return (K) Double.valueOf(Convert.ToDouble(val));
		} else if (clazz.equals(String.class)) {
			return (K) val;
		} else if (clazz.equals(Utf8.class)) {
			return (K) new Utf8(val);
		}
		throw new RuntimeException("Can't parse data as class: " + clazz);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static byte[] toBytes(Object o, Schema schema) throws IOException {
		Type type = schema.getType();
		switch (type) {
		case STRING:
			return Bytes.toBytes(((Utf8) o).toString()); // TODO: maybe ((Utf8)o).getBytes(); ?
		case BYTES:
			return ((ByteBuffer) o).array();
		case INT:
			return Bytes.toBytes((Integer) o);
		case LONG:
			return Bytes.toBytes((Long) o);
		case FLOAT:
			return Bytes.toBytes((Float) o);
		case DOUBLE:
			return Bytes.toBytes((Double) o);
		case BOOLEAN:
			return (Boolean) o ? new byte[] { 1 } : new byte[] { 0 };
		case ENUM:
			return new byte[] { (byte) ((Enum<?>) o).ordinal() };
		case RECORD:
			Map<String, SpecificDatumWriter<?>> writerMap = writerMaps.get();
			SpecificDatumWriter writer = writerMap.get(schema.getFullName());
			if (writer == null) {
				writer = new SpecificDatumWriter(schema);
				writerMap.put(schema.getFullName(), writer);
			}

			BinaryEncoderWithStream encoder = encoders.get();
			if (encoder == null) {
				encoder = new BinaryEncoderWithStream(new ByteArrayOutputStream());
				encoders.set(encoder);
			}
			// reset the buffers
			ByteArrayOutputStream os = (ByteArrayOutputStream) encoder.getOut();
			os.reset();

			writer.write(o, encoder);
			encoder.flush();
			return os.toByteArray();
		default:
			throw new RuntimeException("Unknown type: " + type);
		}
	}
}
