/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.stream.processor;

import io.datakernel.async.Stage;
import io.datakernel.stream.*;

import java.util.function.Predicate;

/**
 * Provides you to filter data for sending. It checks predicate's verity for inputting data and if
 * predicate is true sends data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams result set to the destination .
 *
 * @param <T>
 */
public final class StreamFilter<T> implements StreamTransformer<T, T> {
	public static final Predicate<Object> ALWAYS_TRUE = t -> true;

	private final Input input;
	private final Output output;
	private final Predicate<T> predicate;

	// region creators
	private StreamFilter(Predicate<T> predicate) {
		this.predicate = predicate;
		this.input = new Input();
		this.output = new Output();
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param predicate predicate for filtering data
	 */
	public static <T> StreamFilter<T> create(Predicate<T> predicate) {
		return new StreamFilter<>(predicate);
	}
	// endregion

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected Stage<Void> onProducerEndOfStream() {
			return output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			if (predicate.equals(ALWAYS_TRUE)) {
				input.getProducer().produce(dataReceiver);
			} else {
				Predicate<T> predicate = StreamFilter.this.predicate;
				input.getProducer().produce(item -> {
					if (predicate.test(item)) {
						dataReceiver.onData(item);
					}
				});
			}
		}
	}
}
