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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamSerializerTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(10, 20, 30, 40));
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(eventloop, intSerializer())
				.withDefaultBufferSize(14)
				.withMaxMessageSize(14);
		StreamConsumerToList<ByteBuf> consumer = StreamConsumerToList.randomlySuspending(eventloop);

		source.streamTo(serializerStream.getInput());
		serializerStream.getOutput().streamTo(consumer);

		eventloop.run();
		List<ByteBuf> result = consumer.getList();
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(16, result.get(0).limit());
		Assert.assertEquals(16, result.get(1).limit());
		Assert.assertEquals(10, result.get(0).array()[4]);
		Assert.assertEquals(20, result.get(0).array()[9]);
		Assert.assertEquals(30, result.get(0).array()[14]);
		Assert.assertEquals(40, result.get(1).array()[4]);
		for (ByteBuf buf : result) {
			buf.recycle();
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertStatus(END_OF_STREAM, serializerStream.getInput());
		assertStatus(END_OF_STREAM, serializerStream.getOutput());
	}

	@Test
	public void test2() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(eventloop, intSerializer())
				.withDefaultBufferSize(1);
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(eventloop, intSerializer());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.oneByOne(eventloop, list);

		source.streamTo(serializerStream.getInput());
		serializerStream.getOutput().streamTo(deserializerStream.getInput());
		deserializerStream.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 2, 3), consumer.getList());
		assertStatus(END_OF_STREAM, source);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertStatus(END_OF_STREAM, serializerStream.getInput());
		assertStatus(END_OF_STREAM, serializerStream.getOutput());

		assertStatus(END_OF_STREAM, deserializerStream.getInput());
		assertStatus(END_OF_STREAM, deserializerStream.getOutput());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(eventloop, intSerializer())
				.withDefaultBufferSize(1);
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(eventloop, intSerializer());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.oneByOne(eventloop, list);

		source.streamTo(serializerStream.getInput());
		eventloop.run();

		serializerStream.getOutput().streamTo(deserializerStream.getInput());
		eventloop.run();

		deserializerStream.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3), consumer.getList());
		assertStatus(END_OF_STREAM, source);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertStatus(END_OF_STREAM, serializerStream.getInput());
		assertStatus(END_OF_STREAM, serializerStream.getOutput());

		assertStatus(END_OF_STREAM, deserializerStream.getInput());
		assertStatus(END_OF_STREAM, deserializerStream.getOutput());
	}

	@Test
	public void testProducerWithError() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source = StreamProducers.closingWithError(new ExpectedException("Test Exception"));
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(eventloop, intSerializer())
				.withDefaultBufferSize(1);
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(eventloop, intSerializer());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.oneByOne(eventloop, list);

		source.streamTo(serializerStream.getInput());
		serializerStream.getOutput().streamTo(deserializerStream.getInput());
		deserializerStream.getOutput().streamTo(consumer);

		eventloop.run();
		assertStatus(CLOSED_WITH_ERROR, consumer);
//		assertStatus(CLOSED_WITH_ERROR, source);

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());

		assertStatus(CLOSED_WITH_ERROR, serializerStream.getInput());
		assertStatus(CLOSED_WITH_ERROR, serializerStream.getOutput());

		assertStatus(CLOSED_WITH_ERROR, deserializerStream.getInput());
		assertStatus(CLOSED_WITH_ERROR, deserializerStream.getOutput());
	}

}
