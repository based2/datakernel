package io.datakernel.serial;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ExpectedException;
import io.datakernel.exception.ParseException;

public abstract class ByteBufsSupplier implements Cancellable {
	public static final Exception CLOSED_EXCEPTION = new ExpectedException();
	public static final Exception UNEXPECTED_DATA_EXCEPTION = new ParseException("Unexpected data after end-of-stream");
	public static final Exception UNEXPECTED_END_OF_STREAM_EXCEPTION = new ParseException("Unexpected end-of-stream");

	public final ByteBufQueue bufs;

	protected ByteBufsSupplier(ByteBufQueue bufs) {this.bufs = bufs;}

	protected ByteBufsSupplier() {this.bufs = new ByteBufQueue();}

	public abstract Stage<Void> get();

	public abstract Stage<Void> markEndOfStream();

	public static ByteBufsSupplier ofSupplier(SerialSupplier<ByteBuf> input) {
		return new ByteBufsSupplier() {
			private boolean closed;

			@Override
			public Stage<Void> get() {
				return input.get()
						.thenCompose(buf -> {
							if (closed) {
								if (buf != null) buf.recycle();
								return Stage.ofException(CLOSED_EXCEPTION);
							}
							if (buf != null) {
								bufs.add(buf);
								return Stage.complete();
							} else {
								return Stage.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
							}
						});
			}

			@Override
			public Stage<Void> markEndOfStream() {
				if (!bufs.isEmpty()) {
					return Stage.ofException(UNEXPECTED_DATA_EXCEPTION);
				}
				return input.get()
						.thenCompose(buf -> {
							if (closed) {
								if (buf != null) buf.recycle();
								return Stage.ofException(CLOSED_EXCEPTION);
							}
							if (buf != null) {
								buf.recycle();
								return Stage.ofException(UNEXPECTED_DATA_EXCEPTION);
							} else {
								return Stage.complete();
							}
						});
			}

			@Override
			public void closeWithError(Throwable e) {
				closed = true;
				bufs.recycle();
				input.closeWithError(e);
			}
		};
	}

	public static ByteBufsSupplier ofProvidedQueue(ByteBufQueue queue,
			AsyncSupplier<Void> get, AsyncSupplier<Void> markEndOfStream, Cancellable cancellable) {
		return new ByteBufsSupplier(queue) {
			boolean firstTime = true;

			@Override
			public Stage<Void> get() {
				if (firstTime && !queue.isEmpty()) {
					firstTime = false;
					return Stage.complete();
				}
				return get.get();
			}

			@Override
			public Stage<Void> markEndOfStream() {
				return markEndOfStream.get();
			}

			@Override
			public void closeWithError(Throwable e) {
				cancellable.closeWithError(e);
			}
		};
	}
}
