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

import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkState;

/**
 * It is {@link AbstractStreamTransformer_1_1} which unions all input streams and streams it
 * combination to the destination.
 *
 * @param <T> type of output data
 */
public final class StreamUnion<T> implements HasOutput<T>, HasInputs {
	private final List<Input> inputs = new ArrayList<>();
	private final Output output;

	// region creators
	private StreamUnion() {
		this.output = new Output();
	}

	public static <T> StreamUnion<T> create() {
		return new StreamUnion<T>();
	}

	@Override
	public List<? extends StreamConsumer<?>> getInputs() {
		return inputs;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	public StreamConsumer<T> newInput() {
		checkState(output.getStatus().isOpen());
		Input input = new Input();
		inputs.add(input);
		return input;
	}

	// endregion

	private final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			if (inputs.stream().allMatch(input -> input.getStatus() == StreamStatus.END_OF_STREAM)) {
				output.sendEndOfStream();
			}
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	private final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onSuspended() {
			for (int i = 0; i < inputs.size(); i++) {
				inputs.get(i).getProducer().suspend();
			}
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			if (!inputs.isEmpty()) {
				for (int i = 0; i < inputs.size(); i++) {
					inputs.get(i).getProducer().produce(dataReceiver);
				}
			} else {
				eventloop.post(this::sendEndOfStream);
			}
		}

		@Override
		protected void onError(Throwable t) {
			inputs.forEach(input -> input.closeWithError(t));
		}
	}

}
