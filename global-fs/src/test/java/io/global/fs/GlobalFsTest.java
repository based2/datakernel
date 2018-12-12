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

package io.global.fs;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.HttpResponse;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.LoggingRule.LoggerConfig;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.http.AsyncHttpServer.DEFAULT_ERROR_FORMATTER;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.stream.processor.ByteBufRule.IgnoreLeaks;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.TestUtils.await;
import static io.global.common.TestUtils.awaitException;
import static io.global.common.api.SharedKeyStorage.NO_SHARED_KEY;
import static io.global.fs.api.GlobalFsNode.UPLOADING_TO_TOMBSTONE;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class GlobalFsTest {
	public static final String FILENAME = "folder/test.txt";
	public static final String SIMPLE_CONTENT = "hello world, this is some string of bytes for Global-FS testing";
	private static final RawServerId FIRST_ID = new RawServerId("127.0.0.1:1001");
	private static final RawServerId SECOND_ID = new RawServerId("127.0.0.1:1002");

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor = Executors.newSingleThreadExecutor();

	private DiscoveryService discoveryService;

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private NodeFactory<GlobalFsNode> clientFactory;

	private LocalGlobalFsNode rawFirstClient;
	private LocalGlobalFsNode rawSecondClient;

	private GlobalFsNode firstClient;
	private GlobalFsDriver firstDriver;
	private FsClient firstAliceAdapter;
	private FsClient secondAliceAdapter;
	private FsClient storage;
	private Path dir;

	private GlobalFsNode cachingNode;
	private GlobalFsDriver driver;
	private FsClient aliceGateway;
	private FsClient bobGateway;

	private static String folderFor(RawServerId serverId) {
		return "server_" + serverId.getServerIdString().split(":")[1];
	}

	@Before
	public void setUp() throws IOException {
		dir = temporaryFolder.newFolder().toPath();
		System.out.println("DIR: " + dir);

		storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), executor, dir);
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage.subfolder("discovery"));

		Map<RawServerId, GlobalFsNode> nodes = new HashMap<>();

		clientFactory = new NodeFactory<GlobalFsNode>() {
			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return wrapWithHttpInterface(nodes.computeIfAbsent(serverId, id ->
						LocalGlobalFsNode.create(serverId, discoveryService, this, storage.subfolder(folderFor(id)))), serverId);
			}
		};
		firstClient = clientFactory.create(FIRST_ID);
		GlobalFsNode secondClient = clientFactory.create(SECOND_ID);

		rawFirstClient = (LocalGlobalFsNode) nodes.get(FIRST_ID);
		rawSecondClient = (LocalGlobalFsNode) nodes.get(SECOND_ID);

		firstDriver = GlobalFsDriver.create(firstClient, discoveryService, list(alice, bob), CheckpointPosStrategy.of(10));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, discoveryService, list(alice, bob), CheckpointPosStrategy.of(15));

		firstAliceAdapter = firstDriver.gatewayFor(alice.getPubKey());
		secondAliceAdapter = secondDriver.gatewayFor(alice.getPubKey());

		cachingNode = clientFactory.create(new RawServerId("127.0.0.1:1003"));
		driver = GlobalFsDriver.create(cachingNode, discoveryService, asList(alice, bob), CheckpointPosStrategy.of(16));
		aliceGateway = driver.gatewayFor(alice.getPubKey());
		bobGateway = driver.gatewayFor(bob.getPubKey());
	}

	@SuppressWarnings("Duplicates") // stolen piece from HttpServerConnection
	private GlobalFsNode wrapWithHttpInterface(GlobalFsNode node, RawServerId serverId) {
		GlobalFsNodeServlet servlet = GlobalFsNodeServlet.create(node);
		return HttpGlobalFsNode.create("http://" + serverId.getServerIdString(), request -> {
			Promise<HttpResponse> servletResult;
			try {
				servletResult = servlet.serve(request);
			} catch (UncheckedException u) {
				servletResult = Promise.ofException(u.getCause());
			} catch (ParseException e) {
				servletResult = Promise.ofException(e);
			}
			return servletResult.thenComposeEx((res, e) -> {
				if (e == null) {
					return Promise.of(res);
				}
				return Promise.of(DEFAULT_ERROR_FORMATTER.formatException(e));
			});
		});
	}

	private void announce(KeyPair keys, Set<RawServerId> rawServerIds) {
		await(discoveryService.announce(keys.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), AnnounceData.of(123, rawServerIds), keys.getPrivKey())));
	}

	@Test
	public void cutters() {
		announce(alice, set(FIRST_ID));

		await(ChannelSupplier.of(
				wrapUtf8("hello, this is a test buffer data #01\n"),
				wrapUtf8("hello, this is a test buffer data #02\n"),
				wrapUtf8("hello, this is a test buffer data #03\n"),
				wrapUtf8("hello, this is a test buffer data #04\n"),
				wrapUtf8("hello, this is a test buffer data #05\n"),
				wrapUtf8("hello, this is a test buffer data #06\n"),
				wrapUtf8("hello, this is a test buffer data #07\n"),
				wrapUtf8("hello, this is a test buffer data #08\n"),
				wrapUtf8("hello, this is a test buffer data #09\n"),
				wrapUtf8("hello, this is a test buffer data #10\n"),
				wrapUtf8("hello, this is a test buffer data #11\n"),
				wrapUtf8("hello, this is a test buffer data #12\n"),
				wrapUtf8("hello, this is a test buffer data #13\n"),
				wrapUtf8("hello, this is a test buffer data #14\n"),
				wrapUtf8("hello, this is a test buffer data #15\n")
		).streamTo(await(firstAliceAdapter.upload("test1.txt"))));


		String res = await(await(firstAliceAdapter.download("test1.txt", 10, 380 - 10 - 19)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

		assertEquals("s is a test buffer data #01\n" +
						"hello, this is a test buffer data #02\n" +
						"hello, this is a test buffer data #03\n" +
						"hello, this is a test buffer data #04\n" +
						"hello, this is a test buffer data #05\n" +
						"hello, this is a test buffer data #06\n" +
						"hello, this is a test buffer data #07\n" +
						"hello, this is a test buffer data #08\n" +
						"hello, this is a test buffer data #09\n" +
						"hello, this is a te",
				res);

		res = await(await(firstAliceAdapter.download("test1.txt", 64, 259)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals("er data #02\n" +
						"hello, this is a test buffer data #03\n" +
						"hello, this is a test buffer data #04\n" +
						"hello, this is a test buffer data #05\n" +
						"hello, this is a test buffer data #06\n" +
						"hello, this is a test buffer data #07\n" +
						"hello, this is a test buffer data #08\n" +
						"hello, this is a te",
				res);

		res = await(await(firstAliceAdapter.download("test1.txt", 228, 37)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals("hello, this is a test buffer data #07", res);

		res = await(await(firstAliceAdapter.download("test1.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(570, res.length());
	}

	@Test
	public void upload() throws IOException {
		// simply upload straight to the first node (no masters so it'll cache)
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		byte[] data = Files.readAllBytes(dir
				.resolve(folderFor(FIRST_ID))
				.resolve("data")
				.resolve(alice.getPubKey().asString())
				.resolve(FILENAME));

		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), data);
	}

	@Test
	public void announcedUpload() {
		announce(alice, set(FIRST_ID, SECOND_ID));

		// simply upload straight to the first node (no masters so it'll cache)
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(aliceGateway.upload(FILENAME))));

		int found = 0;

		try {
			Files.readAllBytes(dir
					.resolve(folderFor(FIRST_ID))
					.resolve("data")
					.resolve(alice.getPubKey().asString())
					.resolve(FILENAME));
			found++;
		} catch (Exception ignored) {
		}
		try {
			Files.readAllBytes(dir
					.resolve(folderFor(SECOND_ID))
					.resolve("data")
					.resolve(alice.getPubKey().asString())
					.resolve(FILENAME));
			found++;
		} catch (Exception ignored) {
		}

		assertEquals(1, found);
	}

	@Test
	public void proxyUpload() {
		// first node is master
		announce(alice, set(FIRST_ID));

		// upload to master through second node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(secondAliceAdapter.upload(FILENAME))));

		// download from first
		byte[] res = await(await(firstAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();
		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), res);

		// download from second
		res = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();
		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), res);
	}

	@Test
	public void offsetUpload() {
		// upload some data
		await(ChannelSupplier.of(wrapUtf8("first line of the content\n")).streamTo(await(firstAliceAdapter.upload(FILENAME))));
		// and then upload more data, maintaining the immutablility
		await(ChannelSupplier.of(wrapUtf8("ntent\nsecond line, appended\n")).streamTo(await(firstAliceAdapter.upload(FILENAME, 20))));

		// download the whole file back
		byte[] res = await(await(firstAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();
		String expected = "first line of the content\nsecond line, appended\n";
		assertArrayEquals(expected.getBytes(UTF_8), res);

		// it should have the proper size
		FileMetadata meta = await(firstAliceAdapter.getMetadata(FILENAME));
		assertEquals(expected.length(), meta.getSize());
	}

	@Test
	public void download() {
		// upload to the node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// and then download back
		byte[] data = await(await(firstAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();

		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), data);
	}

	@Test
	@IgnoreLeaks("sometimes it fails, TODO") // TODO anton: fix this
	public void proxyDownload() {
		// first node is master
		announce(alice, set(FIRST_ID));

		// and it has data
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// download from second
		byte[] data = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();

		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), data);
	}

	@Test
	public void delete() {
		// upload to the node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// and delete
		await(firstAliceAdapter.delete(FILENAME));

		// file should not be found
		assertSame(FILE_NOT_FOUND, awaitException(firstAliceAdapter.download(FILENAME)));
	}

	@Test
	public void proxyDelete() {
		// first node is master
		announce(alice, set(FIRST_ID));

		// upload to the node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// delete through the second node
		await(secondAliceAdapter.delete(FILENAME));

		// file should not be found
		assertSame(FILE_NOT_FOUND, awaitException(firstAliceAdapter.download(FILENAME)));
	}

	@Test
	public void cacheDelete() {
		// first node is master
		announce(alice, set(FIRST_ID));

		// upload to first
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// download and cache on second
		byte[] res = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();
		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), res);

		// delete on first
		await(firstAliceAdapter.delete(FILENAME));

		// second should check the actual metadata when trying to download
		assertSame(FILE_NOT_FOUND, awaitException(secondAliceAdapter.download(FILENAME)));
	}

	@Test
	public void separate() {
		FsClient firstBobAdapter = firstDriver.gatewayFor(bob.getPubKey());

		// upload to Alice's space
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// and download back
		String res = await(await(firstAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(SIMPLE_CONTENT, res);

		// Bob's space has nothing
		assertSame(FILE_NOT_FOUND, awaitException(firstBobAdapter.download(FILENAME)));
	}

	@Test
	public void downloadOnlyCheckpoint() {
		// downloading 0 bytes at the checkpoint position should return only a one frame with that checkpoint
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT))
				.transformWith(FrameSigner.create(alice.getPrivKey(), CheckpointPosStrategy.of(4), GlobalFsTest.FILENAME, 0, new SHA256Digest(), null))
				.streamTo(await(firstClient.upload(alice.getPubKey(), GlobalFsTest.FILENAME, -1))));

		List<DataFrame> list = await(await(firstClient.download(alice.getPubKey(), GlobalFsTest.FILENAME, 4, 0)).toList());
		assertEquals(1, list.size());
		DataFrame frame = list.get(0);
		assertTrue(frame.isCheckpoint());
		assertTrue(frame.getCheckpoint().verify(alice.getPubKey()));
		assertEquals(4, frame.getCheckpoint().getValue().getPosition());
	}

	@Test
	public void catchUp() {
		announce(alice, set(FIRST_ID, SECOND_ID));

		// upload file to the first node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// just ping the second node with alice's pubkey so it would check if it it's master
		await(secondAliceAdapter.list());

		// // second node never even heard of alice to know it is it's master
		// rawSecondClient.withManagedPubKeys(set(alice.getPubKey()));

		// make second one catch up
		await(rawSecondClient.catchUp());

		// now second should have this file too
		String res = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(SIMPLE_CONTENT, res);
	}

	@Test
	public void catchUpPartialFiles() {
		String part = "hello, this is a test little string of bytes";
		String string = part + "\nwhich has a second line by the way, hello there";

		announce(alice, set(FIRST_ID, SECOND_ID));

		// upload half of the text to the second node
		await(ChannelSupplier.of(wrapUtf8(part)).streamTo(await(secondAliceAdapter.upload(FILENAME))));

		// and the whole text to the first one
		await(ChannelSupplier.of(wrapUtf8(string)).streamTo(await(firstAliceAdapter.upload(FILENAME, 0))));

		// make second one catch up
		await(rawSecondClient.catchUp());

		// now second should have the whole text too
		String res = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(string, res);
	}

	@Test
	@LoggerConfig(logger = "io.global.fs", value = "TRACE")
	@LoggerConfig(logger = "io.global.fs.local.RemoteFsCheckpointStorage", value = "INFO")
	public void catchUpTombstones() {
		announce(alice, set(FIRST_ID, SECOND_ID));

		// uploading one string of bytes to first node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// deleting the file on second node
		await(secondAliceAdapter.delete(FILENAME));

		// make second node catch up
		await(rawFirstClient.catchUp());

		// uploading on the first node again
		Throwable e = awaitException(firstAliceAdapter.upload(FILENAME));

		assertSame(UPLOADING_TO_TOMBSTONE, e);
	}

	@Test
	public void push() {
		// upload file to the first node
		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(firstAliceAdapter.upload(FILENAME))));

		// make second node master
		announce(alice, set(SECOND_ID));

		// the push
		await(rawFirstClient.push());

		byte[] data = await(await(secondAliceAdapter.download(FILENAME)).toCollector(ByteBufQueue.collector())).asArray();

		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), data);
	}

	@Test
	public void biggerTest() {
		announce(alice, set(FIRST_ID));
		announce(bob, set(FIRST_ID, SECOND_ID));

		String doubleContent = SIMPLE_CONTENT + '\n' + SIMPLE_CONTENT;

		await(ChannelSupplier.of(wrapUtf8(SIMPLE_CONTENT)).streamTo(await(aliceGateway.upload("first.txt"))));
		await(ChannelSupplier.of(wrapUtf8(doubleContent)).streamTo(await(bobGateway.upload("second.txt"))));

		// download (and cache) from other node
		byte[] data = await(await(secondAliceAdapter.download("first.txt")).toCollector(ByteBufQueue.collector())).asArray();
		assertArrayEquals(SIMPLE_CONTENT.getBytes(UTF_8), data);

		// fetch on the first node so it is up-to-date
		await(rawFirstClient.fetch());

		//
		data = await(await(firstDriver.gatewayFor(bob.getPubKey()).download("second.txt")).toCollector(ByteBufQueue.collector())).asArray();
		assertArrayEquals(doubleContent.getBytes(UTF_8), data);
	}

	@Test
	@IgnoreLeaks("TODO") // TODO anton: fix this
	public void encryption() {
		SimKey key1 = SimKey.generate();
		SimKey key2 = SimKey.generate();

		firstDriver.getPrivateKeyStorage().changeCurrentSimKey(key1);

		String filename = FILENAME;
		String data = "some plain ASCII data to be uploaded and encrypted";

		// upload on first
		await(ChannelSupplier.of(wrapUtf8(data)).streamTo(await(firstAliceAdapter.upload(filename))));

		// and download back
		String res = await(await(firstAliceAdapter.download(filename, 12, 32)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

		// check that encryption-decryption worked
		assertEquals(data.substring(12, 12 + 32), res);

		// pretend we try to download "someone else's" file
		firstDriver.getPrivateKeyStorage().forget(key1);
		firstDriver.getPrivateKeyStorage().changeCurrentSimKey(key2);

		assertSame(NO_SHARED_KEY, awaitException(firstAliceAdapter.download(filename)));

		// share "someone else's" file key with ourselves
		await(discoveryService.shareKey(alice.getPubKey(),
				SignedData.sign(REGISTRY.get(SharedSimKey.class), SharedSimKey.of(key1, alice.getPubKey()), alice.getPrivKey())));

		// and now we can download
		res = await(await(firstAliceAdapter.download(filename)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

		// and decryption works
		assertEquals(data, res);
	}
}
