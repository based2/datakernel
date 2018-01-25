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

package io.datakernel.remotefs;

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.remotefs.RemoteFsCommands.Download;
import io.datakernel.remotefs.RemoteFsCommands.FsCommand;
import io.datakernel.remotefs.RemoteFsResponses.FsResponse;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.net.Messaging.ReceiveMessageCallback;
import io.datakernel.stream.net.MessagingSerializer;
import io.datakernel.stream.net.MessagingWithBinaryStreaming;
import io.datakernel.stream.stats.StreamStats;
import io.datakernel.stream.stats.StreamStatsDetailed;
import io.datakernel.stream.stats.StreamStatsForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.Stages.onError;
import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.net.MessagingSerializers.ofJson;
import static io.datakernel.stream.stats.StreamStatsSizeCounter.forByteBufs;

public final class RemoteFsClient implements IRemoteFsClient {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop;
	protected final SocketSettings socketSettings = SocketSettings.create();

	// SSL
	protected final SSLContext sslContext;
	protected final ExecutorService sslExecutor;

	private final InetSocketAddress address;

	private final MessagingSerializer<FsResponse, FsCommand> serializer =
			ofJson(RemoteFsResponses.adapter, RemoteFsCommands.adapter);

	// creators & builders
	protected RemoteFsClient(Eventloop eventloop, InetSocketAddress address,
	                         SSLContext sslContext, ExecutorService sslExecutor) {
		this.eventloop = eventloop;
		this.address = address;
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
	}

	public static RemoteFsClient create(Eventloop eventloop, InetSocketAddress address) {
		return new RemoteFsClient(eventloop, address, null, null);
	}

	public RemoteFsClient withSslEnabled(SSLContext sslContext, ExecutorService sslExecutor) {
		return new RemoteFsClient(eventloop, address, sslContext, sslExecutor);
	}
	// endregion

	@Override
	public CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> upload(String fileName) {
		return connect(address).thenApply(messaging -> {
			messaging.send(new RemoteFsCommands.Upload(fileName));
			StreamConsumerWithResult<ByteBuf, Void> consumer = messaging.sendBinaryStream();
			SettableStage<Void> ack = SettableStage.create();

			messaging.receive(new ReceiveMessageCallback<FsResponse>() {
				@Override
				public void onReceive(FsResponse msg) {
					if (msg instanceof RemoteFsResponses.Acknowledge) {
						messaging.close();
						ack.set(null);
					} else if (msg instanceof RemoteFsResponses.Err) {
						ack.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).getMsg()));
					} else {
						ack.setException(new RemoteFsException("Invalid message received: " + msg));
					}
				}

				@Override
				public void onReceiveEndOfStream() {
					ack.setException(new RemoteFsException("Unexpected end of stream for: " + fileName));
				}

				@Override
				public void onException(Exception e) {
					ack.setException(e);
				}
			});

			StreamConsumerWithResult<ByteBuf, Void> consumerWithResult = consumer.withResult(ack);
			consumerWithResult.getResult().whenComplete(onError($ -> messaging.close()));
			return consumerWithResult.withLateBinding();
		});
	}

	@Override
	public CompletionStage<StreamProducerWithResult<ByteBuf, Void>> download(String fileName, long startPosition) {
		return connect(address).thenCompose(messaging -> {
			SettableStage<StreamProducerWithResult<ByteBuf, Void>> stage = SettableStage.create();

			messaging.send(new Download(fileName, startPosition)).thenAccept($ ->
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							if (msg instanceof RemoteFsResponses.Ready) {
								long size = ((RemoteFsResponses.Ready) msg).getSize();

								StreamProducerWithResult<ByteBuf, Void> producer = messaging.receiveBinaryStream();
								StreamStatsDetailed stats = StreamStats.detailed(forByteBufs());
								StreamStatsForwarder<ByteBuf> sizeForwarder = StreamStatsForwarder.create(stats);

								SettableStage<Void> ack = SettableStage.create();
								stream(producer, sizeForwarder.getInput())
										.thenAccept($ -> {
											messaging.close();
											if (stats.getTotalSize() == size - startPosition) {
												ack.set(null);
											} else {
												ack.setException(new IOException("Invalid stream size for '" + fileName + "' starting from " + startPosition +
														", expected: " + (size - startPosition) + " actual: " + stats.getTotalSize()));
											}
										});

								stage.set(sizeForwarder.getOutput()
										.withResult(ack)
										.withLateBinding());
							} else if (msg instanceof RemoteFsResponses.Err) {
								stage.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).getMsg()));
							} else {
								stage.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							stage.setException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							stage.setException(new RemoteFsException(e));
						}
					})
			);

			return stage.whenComplete(onError($ -> messaging.close()));
		});
	}

	@Override
	public CompletionStage<Void> delete(String fileName) {
		return connect(address).thenCompose(messaging -> {
			SettableStage<Void> ack = SettableStage.create();
			messaging.send(new RemoteFsCommands.Delete(fileName)).whenComplete(($, throwable) -> {
				if (throwable == null) {
					logger.trace("command to delete {} send", fileName);
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof RemoteFsResponses.Ok) {
								messaging.close();
								ack.set(null);
							} else if (msg instanceof RemoteFsResponses.Err) {
								messaging.close();
								ack.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).getMsg()));
							} else {
								messaging.close();
								ack.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							ack.setException(new RemoteFsException("Unexpected end of stream for: " + fileName));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							ack.setException(e);
						}
					});
				} else {
					messaging.close();
					ack.setException(throwable);
				}
			});
			return ack;
		});
	}

	@Override
	public CompletionStage<List<String>> list() {
		return connect(address).thenCompose(messaging -> {
			SettableStage<List<String>> ack = SettableStage.create();
			messaging.send(new RemoteFsCommands.ListFiles()).whenComplete(($, throwable) -> {
				if (throwable == null) {
					logger.trace("command to list files send");
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof RemoteFsResponses.ListOfFiles) {
								messaging.close();
								ack.set(((RemoteFsResponses.ListOfFiles) msg).getFiles());
							} else if (msg instanceof RemoteFsResponses.Err) {
								messaging.close();
								ack.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).getMsg()));
							} else {
								messaging.close();
								ack.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							ack.setException(new RemoteFsException("Unexpected end of stream while trying to list files"));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							ack.setException(e);
						}
					});
				} else {
					messaging.close();
					ack.setException(throwable);
				}
			});
			return ack;
		});
	}

	@Override
	public CompletionStage<Void> move(Map<String, String> changes) {
		return connect(address).thenCompose(messaging -> {
			SettableStage<Void> ack = SettableStage.create();
			messaging.send(new RemoteFsCommands.Move(changes)).whenComplete((aVoid, throwable) -> {
				if (throwable == null) {
					logger.trace("command move files send");
					messaging.receive(new ReceiveMessageCallback<FsResponse>() {
						@Override
						public void onReceive(FsResponse msg) {
							logger.trace("received {}", msg);
							if (msg instanceof RemoteFsResponses.Ok) {
								messaging.close();
								ack.set(null);
							} else if (msg instanceof RemoteFsResponses.Err) {
								messaging.close();
								ack.setException(new RemoteFsException(((RemoteFsResponses.Err) msg).getMsg()));
							} else {
								messaging.close();
								ack.setException(new RemoteFsException("Invalid message received: " + msg));
							}
						}

						@Override
						public void onReceiveEndOfStream() {
							logger.warn("received unexpected end of stream");
							messaging.close();
							ack.setException(new RemoteFsException("Unexpected end of stream for: " + changes));
						}

						@Override
						public void onException(Exception e) {
							messaging.close();
							ack.setException(e);
						}
					});
				} else {
					messaging.close();
					ack.setException(throwable);
				}
			});
			return ack;
		});
	}

	private CompletionStage<MessagingWithBinaryStreaming<FsResponse, FsCommand>> connect(InetSocketAddress address) {
		return eventloop.connect(address).thenApply(socketChannel -> {
			AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
			AsyncTcpSocket asyncTcpSocket = sslContext != null
					? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor)
					: asyncTcpSocketImpl;
			MessagingWithBinaryStreaming<FsResponse, FsCommand> messaging =
					MessagingWithBinaryStreaming.create(asyncTcpSocket, serializer);
			asyncTcpSocket.setEventHandler(messaging);
			asyncTcpSocketImpl.register();
			return messaging;
		});
	}
}