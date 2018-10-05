/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.http;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.AsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.AsyncTimeoutException;
import io.datakernel.exception.ParseException;
import io.datakernel.http.stream.*;
import io.datakernel.serial.*;
import io.datakernel.util.ApplicationSettings;
import io.datakernel.util.MemSize;

import java.util.function.BiConsumer;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpHeaderValue.ofBytes;
import static io.datakernel.http.HttpHeaderValue.ofDecimal;
import static io.datakernel.http.HttpHeaders.*;
import static java.lang.Math.max;

@SuppressWarnings("ThrowableInstanceNeverThrown")
public abstract class AbstractHttpConnection {
	public static final AsyncTimeoutException READ_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection read timeout");
	public static final AsyncTimeoutException WRITE_TIMEOUT_ERROR = new AsyncTimeoutException("HTTP connection write timeout");
	public static final ParseException CLOSED_CONNECTION = new ParseException("HTTP connection unexpectedly closed");
	public static final ParseException HEADER_NAME_ABSENT = new ParseException("Header name is absent");
	public static final ParseException TOO_BIG_HTTP_MESSAGE = new ParseException("Too big HttpMessage");
	public static final ParseException TOO_LONG_HEADER = new ParseException("Header line exceeds max header size");
	public static final ParseException TOO_MANY_HEADERS = new ParseException("Too many headers");
	public static final ParseException INCOMPLETE_MESSAGE = new ParseException("Incomplete HTTP message");
	public static final ParseException UNEXPECTED_READ = new ParseException("Unexpected read data");

	public static final SerialConsumer<ByteBuf> BUF_RECYCLER = SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle));

	public static final MemSize MAX_HEADER_LINE_SIZE = MemSize.of(ApplicationSettings.getInt(HttpMessage.class, "maxHeaderLineSize", MemSize.kilobytes(8).toInt())); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADER_LINE_SIZE_BYTES = MAX_HEADER_LINE_SIZE.toInt(); // http://stackoverflow.com/questions/686217/maximum-on-http-header-values
	public static final int MAX_HEADERS = ApplicationSettings.getInt(HttpMessage.class, "maxHeaders", 100); // http://httpd.apache.org/docs/2.2/mod/core.html#limitrequestfields

	protected static final HttpHeaderValue CONNECTION_KEEP_ALIVE_HEADER = HttpHeaderValue.of("keep-alive");
	protected static final HttpHeaderValue CONNECTION_CLOSE_HEADER = HttpHeaderValue.of("close");

	private static final byte[] CONNECTION_KEEP_ALIVE = encodeAscii("keep-alive");
	private static final byte[] TRANSFER_ENCODING_CHUNKED = encodeAscii("chunked");

	protected final Eventloop eventloop;

	protected final AsyncTcpSocket socket;
	protected final ByteBufQueue readQueue = new ByteBufQueue();

	protected static final byte KEEP_ALIVE = 1 << 0;
	protected static final byte GZIPPED = 1 << 1;
	protected static final byte CHUNKED = 1 << 2;
	protected static final byte BODY_RECEIVED = 1 << 3;
	protected static final byte BODY_SENT = 1 << 4;
	protected byte flags = 0;

	protected int numberOfKeepAliveRequests;

	protected static final byte[] CONTENT_ENCODING_GZIP = encodeAscii("gzip");

	protected int contentLength;

	@Nullable
	ConnectionsLinkedList pool;
	@Nullable
	AbstractHttpConnection prev;
	@Nullable
	AbstractHttpConnection next;
	long poolTimestamp;

	/**
	 * Creates a new instance of AbstractHttpConnection
	 *
	 * @param eventloop eventloop which will handle its I/O operations
	 */
	public AbstractHttpConnection(Eventloop eventloop, AsyncTcpSocket socket) {
		this.eventloop = eventloop;
		this.socket = socket;
	}

	protected abstract void onFirstLine(byte[] line, int size) throws ParseException;

	protected abstract void onHeadersReceived(ByteBuf body, SerialSupplier<ByteBuf> bodySupplier);

	protected abstract void onBodyReceived();

	protected abstract void onBodySent();

	protected abstract void onClosed();

	public abstract void onClosedWithError(Throwable e);

	protected final boolean isClosed() {
		return pool == null;
	}

	public final void close() {
		if (isClosed()) return;
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	protected final void closeWithError(Throwable e) {
		if (isClosed()) return;
		onClosedWithError(e);
		onClosed();
		socket.close();
		readQueue.recycle();
	}

	static SerialSupplier<ByteBuf> bodySupplier(HttpMessage httpMessage) {
		if (httpMessage.body != null) {
			ByteBuf body = httpMessage.body;
			assert httpMessage.bodySupplier == null;
			if (!httpMessage.useGzip) {
				httpMessage.setHeader(CONTENT_LENGTH, ofDecimal(body.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + body.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(body);
				body.recycle();
				httpMessage.body = null;
				return SerialSupplier.of(buf);
			} else {
				ByteBuf gzippedBody = GzipProcessorUtils.toGzip(body);
				httpMessage.setHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
				httpMessage.setHeader(CONTENT_LENGTH, ofDecimal(gzippedBody.readRemaining()));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize() + gzippedBody.readRemaining());
				httpMessage.writeTo(buf);
				buf.put(gzippedBody);
				gzippedBody.recycle();
				httpMessage.body = null;
				return SerialSupplier.of(buf);
			}
		} else if (httpMessage.bodySupplier != null) {
			httpMessage.setHeader(TRANSFER_ENCODING, ofBytes(TRANSFER_ENCODING_CHUNKED));
			if (!httpMessage.useGzip) {
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
				httpMessage.writeTo(buf);
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				chunker.getInput().set(httpMessage.bodySupplier);
				SerialSupplier<ByteBuf> result = SerialSuppliers.concat(SerialSupplier.of(buf), chunker.getOutput().getSupplier());
				chunker.start();
				return result;
			} else {
				httpMessage.setHeader(CONTENT_ENCODING, ofBytes(CONTENT_ENCODING_GZIP));
				ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
				httpMessage.writeTo(buf);
				BufsConsumerGzipDeflater deflater = BufsConsumerGzipDeflater.create();
				BufsConsumerChunkedEncoder chunker = BufsConsumerChunkedEncoder.create();
				deflater.getInput().set(httpMessage.bodySupplier);
				SerialQueue<ByteBuf> queue = new SerialZeroBuffer<>();
				deflater.getOutput().set(queue.getConsumer());
				chunker.getInput().set(queue.getSupplier());
				SerialSupplier<ByteBuf> result = SerialSuppliers.concat(SerialSupplier.of(buf), chunker.getOutput().getSupplier());
				deflater.start();
				chunker.start();
				return result;
			}
		} else {
			httpMessage.setHeader(CONTENT_LENGTH, ofDecimal(0));
			ByteBuf buf = ByteBufPool.allocate(httpMessage.estimateSize());
			httpMessage.writeTo(buf);
			return SerialSupplier.of(buf);
		}
	}

	protected final void writeHttpMessage(HttpMessage httpMessage) {
		writeHttpMessageImpl(bodySupplier(httpMessage));
		httpMessage.recycle();
	}

	private void writeHttpMessageImpl(SerialSupplier<ByteBuf> bodySupplier) {
		bodySupplier.get()
				.whenComplete((buf, e) -> {
					if (e == null) {
						if (buf != null) {
							socket.write(buf)
									.whenComplete(($, e2) -> {
										if (e2 == null) {
											writeHttpMessageImpl(bodySupplier);
										} else {
											closeWithError(e2);
										}
									});
						} else {
							flags |= BODY_SENT;
							onBodySent();
						}
					} else {
						closeWithError(e);
					}
				});
	}

	@Nullable
	private ByteBuf takeFirstLine() {
		int offset = 0;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.readPosition(); p < buf.writePosition(); p++) {
				if (buf.at(p) == LF) {
					int size = offset + p - buf.readPosition() + 1;
					ByteBuf line = ByteBufPool.allocate(max(16, size)); // allocate at least 16 bytes
					readQueue.drainTo(line, size);
					if (line.readRemaining() >= 2 && line.peek(line.readRemaining() - 2) == CR) {
						line.moveWritePosition(-2);
					} else {
						line.moveWritePosition(-1);
					}
					return line;
				}
			}
			offset += buf.readRemaining();
		}
		return null;
	}

	@Nullable
	private ByteBuf takeHeader() {
		int offset = 0;
		for (int i = 0; i < readQueue.remainingBufs(); i++) {
			ByteBuf buf = readQueue.peekBuf(i);
			for (int p = buf.readPosition(); p < buf.writePosition(); p++) {
				if (buf.at(p) == LF) {

					// check if multiline header(CRLF + 1*(SP|HT)) rfc2616#2.2
					if (isMultilineHeader(buf, p)) {
						preprocessMultiLine(buf, p);
						continue;
					}

					ByteBuf line = readQueue.takeExactSize(offset + p - buf.readPosition() + 1);
					if (line.readRemaining() >= 2 && line.peek(line.readRemaining() - 2) == CR) {
						line.moveWritePosition(-2);
					} else {
						line.moveWritePosition(-1);
					}
					return line;
				}
			}
			offset += buf.readRemaining();
		}
		return null;
	}

	private boolean isMultilineHeader(ByteBuf buf, int p) {
		return p + 1 < buf.writePosition() && (buf.at(p + 1) == SP || buf.at(p + 1) == HT) &&
				isDataBetweenStartAndLF(buf, p);
	}

	private boolean isDataBetweenStartAndLF(ByteBuf buf, int p) {
		return !(p == buf.readPosition() || (p - buf.readPosition() == 1 && buf.at(p - 1) == CR));
	}

	private void preprocessMultiLine(ByteBuf buf, int pos) {
		buf.array()[pos] = SP;
		if (buf.at(pos - 1) == CR) {
			buf.array()[pos - 1] = SP;
		}
	}

	private void onHeader(ByteBuf line) throws ParseException {
		int pos = line.readPosition();
		int hashCode = 1;
		while (pos < line.writePosition()) {
			byte b = line.at(pos);
			if (b == ':')
				break;
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			hashCode = 31 * hashCode + b;
			pos++;
		}
		if (pos == line.writePosition()) throw HEADER_NAME_ABSENT;
		HttpHeader httpHeader = HttpHeaders.of(line.array(), line.readPosition(), pos - line.readPosition(), hashCode);
		pos++;

		// RFC 2616, section 19.3 Tolerant Applications
		while (pos < line.writePosition() && (line.at(pos) == SP || line.at(pos) == HT)) {
			pos++;
		}
		line.readPosition(pos);
		onHeader(httpHeader, line);
	}

	protected void onHeader(HttpHeader header, ByteBuf value) throws ParseException {
		assert !isClosed();
		if (header == CONTENT_LENGTH) {
			contentLength = ByteBufStrings.decodeDecimal(value.array(), value.readPosition(), value.readRemaining());
		} else if (header == CONNECTION) {
			flags = (byte) ((flags & ~KEEP_ALIVE) |
					(equalsLowerCaseAscii(CONNECTION_KEEP_ALIVE, value.array(), value.readPosition(), value.readRemaining()) ? KEEP_ALIVE : 0));
		} else if (header == TRANSFER_ENCODING) {
			flags |= equalsLowerCaseAscii(TRANSFER_ENCODING_CHUNKED, value.array(), value.readPosition(), value.readRemaining()) ? CHUNKED : 0;
		} else if (header == CONTENT_ENCODING) {
			flags |= equalsLowerCaseAscii(CONTENT_ENCODING_GZIP, value.array(), value.readPosition(), value.readRemaining()) ? GZIPPED : 0;
		}
	}

	protected final void readHttpMessage() {
		contentLength = 0;
		readFirstLine();
	}

	private void readFirstLine() {
		assert !isClosed();
		ByteBuf buf = null;
		try {
			buf = takeFirstLine();
			if (buf == null) { // states that more bytes are being required
				if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
				socket.read().whenComplete(firstLineConsumer);
				return;
			}

			onFirstLine(buf.array(), buf.writePosition());
		} catch (ParseException e) {
			closeWithError(e);
			return;
		} finally {
			if (buf != null) {
				buf.recycle();
			}
		}

		readHeaders();
	}

	private void readHeaders() {
		assert !isClosed();
		try {
			while (true) {
				ByteBuf headerBuf = takeHeader();
				if (headerBuf == null) { // states that more bytes are being required
					if (readQueue.hasRemainingBytes(MAX_HEADER_LINE_SIZE_BYTES)) throw TOO_LONG_HEADER;
					socket.read().whenComplete(headersConsumer);
					return;
				}

				if (!headerBuf.canRead()) {
					headerBuf.recycle();
					break;
				}

				onHeader(headerBuf);
			}
		} catch (ParseException e) {
			closeWithError(e);
			return;
		}

		readBody();
	}

	private void readBody() {
		if ((flags & (CHUNKED | GZIPPED)) == 0 && readQueue.hasRemainingBytes(contentLength)) {
			ByteBuf body = readQueue.takeExactSize(contentLength);
			onHeadersReceived(body, null);
			if (isClosed()) return;
			flags |= BODY_RECEIVED;
			onBodyReceived();
			return;
		}

		ByteBufsInput input;
		AsyncProcess transferDecoder;
		SerialOutput<ByteBuf> transferDecoderOutput;

		if ((flags & CHUNKED) == 0) {
			BufsConsumerDelimiter decoder = BufsConsumerDelimiter.create(contentLength);
			transferDecoder = decoder;
			input = decoder.getByteBufsInput();
			transferDecoderOutput = decoder.getOutput();
		} else {
			BufsConsumerChunkedDecoder decoder = BufsConsumerChunkedDecoder.create();
			transferDecoder = decoder;
			input = decoder.getByteBufsInput();
			transferDecoderOutput = decoder.getOutput();
		}

		AsyncProcess contentDecoder;
		SerialOutput<ByteBuf> contentDecoderOutput;

		if ((flags & GZIPPED) == 0) {
			contentDecoder = null;
			contentDecoderOutput = transferDecoderOutput;
		} else {
			BufsConsumerGzipInflater decoder = BufsConsumerGzipInflater.create();
			contentDecoder = decoder;
			contentDecoderOutput = decoder.getOutput();
			SerialQueue<ByteBuf> queue = new SerialZeroBuffer<>();
			transferDecoderOutput.set(queue.getConsumer());
			decoder.getInput().set(queue.getSupplier());
		}

		input.setInput(ByteBufsSupplier.ofProvidedQueue(
				readQueue,
				() -> socket.read()
						.thenComposeEx((buf, e) -> {
							if (e == null) {
								readQueue.add(buf);
								return Stage.complete();
							} else {
								closeWithError(e);
								return Stage.<Void>ofException(e);
							}
						}),
				Stage::complete,
				this::closeWithError));

		onHeadersReceived(null, contentDecoderOutput.getSupplier(new SerialZeroBuffer<>()));
		if (isClosed()) return;

		Stage<Void> stage;
		if (contentDecoder == null) {
			stage = transferDecoder.start();
		} else {
			transferDecoder.start();
			stage = contentDecoder.start();
		}
		stage.whenComplete(($, e) -> {
			if (e == null) {
				flags |= BODY_RECEIVED;
				onBodyReceived();
			} else {
				closeWithError(e);
			}
		});
	}

	protected void switchPool(ConnectionsLinkedList newPool) {
		if (pool != null) pool.removeNode(this);
		pool = newPool;
		if (pool != null) pool.addLastNode(this);
		poolTimestamp = eventloop.currentTimeMillis();
	}

	@Override
	public String toString() {
		return ", socket=" + socket +
				", readQueue=" + readQueue +
				", closed=" + isClosed() +
				", keepAlive=" + ((flags & KEEP_ALIVE) != 0) +
				", isGzipped=" + ((flags & GZIPPED) != 0) +
				", isChunked=" + ((flags & CHUNKED) != 0) +
				", contentLengthRemaining=" + contentLength +
				", poolTimestamp=" + poolTimestamp;
	}

	private abstract class ReadConsumer implements BiConsumer<ByteBuf, Throwable> {
		@Override
		public void accept(ByteBuf buf, Throwable e) {
			assert !isClosed() || e != null;
			if (e == null) {
				if (buf != null) {
					readQueue.add(buf);
					thenRun();
				} else {
					closeWithError(INCOMPLETE_MESSAGE);
				}
			} else {
				closeWithError(e);
			}
		}

		abstract void thenRun();
	}

	private final ReadConsumer firstLineConsumer = new ReadConsumer() {
		@Override
		void thenRun() {
			readFirstLine();
		}
	};

	private final ReadConsumer headersConsumer = new ReadConsumer() {
		@Override
		void thenRun() {
			readHeaders();
		}
	};
}
