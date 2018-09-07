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

package io.datakernel.serial.net;

import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SimpleServer;
import io.datakernel.serial.SerialZeroBuffer;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.INT_SERIALIZER;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public final class SocketStreamingConnectionTest {
	private static final int LISTEN_PORT = 1234;
	private static final InetSocketAddress address;

	static {
		try {
			address = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), LISTEN_PORT);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() throws Exception {
		CompletableFuture<?> future;
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			list.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		SimpleServer server = SimpleServer.create(eventloop,
				asyncTcpSocket -> {
					SocketStreamingConnection connection = SocketStreamingConnection.create(asyncTcpSocket);

					connection.getSocketReader()
							.apply(SerialBinaryDeserializer.create(INT_SERIALIZER))
							.streamTo(consumerToList);

					return connection;
				})
				.withListenAddress(address)
				.withAcceptOnce();
		server.listen();

		future = eventloop.connect(address)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					SocketStreamingConnection connection = SocketStreamingConnection.create(asyncTcpSocket);

					StreamProducer.ofIterable(list)
							.apply(SerialBinarySerializer.create(INT_SERIALIZER)
									.withInitialBufferSize(MemSize.of(1)))
							.streamTo(connection.getSocketWriter());

					asyncTcpSocket.setEventHandler(connection);
					asyncTcpSocket.register();
				})
				.toCompletableFuture();

		eventloop.run();

		future.get();
		assertEquals(list, consumerToList.getList());
	}

	@Test
	public void testLoopback() throws Exception {
		CompletableFuture<?> future;
		List<Integer> source = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			source.add(i);
		}

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		SimpleServer server = SimpleServer.create(eventloop,
				asyncTcpSocket -> {
					SocketStreamingConnection connection = SocketStreamingConnection.create(asyncTcpSocket);
					connection.getSocketReader()
							.streamTo(connection.getSocketWriter());
					return connection;
				})
				.withListenAddress(address)
				.withAcceptOnce();
		server.listen();

		future = eventloop.connect(address)
				.whenResult(socketChannel -> {
					AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
					SocketStreamingConnection connection = SocketStreamingConnection.create(asyncTcpSocket);

					SerialBinarySerializer<Integer> streamSerializer = SerialBinarySerializer.create(INT_SERIALIZER).withInitialBufferSize(MemSize.of(1));
					SerialBinaryDeserializer<Integer> streamDeserializer = SerialBinaryDeserializer.create(INT_SERIALIZER);

					streamSerializer.streamTo(connection.getSocketWriter(), new SerialZeroBuffer<>());
					connection.getSocketReader().streamTo(streamDeserializer);

					StreamProducer.ofIterable(source).streamTo(streamSerializer);
					streamDeserializer.streamTo(consumerToList);

					asyncTcpSocket.setEventHandler(connection);
					asyncTcpSocket.register();
				})
				.toCompletableFuture();

		eventloop.run();

		future.get();
		assertEquals(source, consumerToList.getList());
	}
}
