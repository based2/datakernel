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

package io.global.common.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.ByteArrayIdentity;
import io.global.common.RawServerId;
import io.global.ot.util.BinaryDataFormats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.global.ot.util.BinaryDataFormats.sizeof;

public final class AnnounceData implements ByteArrayIdentity {
	private final byte[] bytes;

	private final long timestamp;
	private final Set<RawServerId> serverIds;

	// region creators
	public AnnounceData(byte[] bytes, long timestamp, Set<RawServerId> serverIds) {
		this.bytes = bytes;
		this.timestamp = timestamp;
		this.serverIds = serverIds;
	}

	public static AnnounceData of(long timestamp, Set<RawServerId> serverIds) {
		List<RawServerId> ids = new ArrayList<>(serverIds);
		ByteBuf buf = ByteBufPool.allocate(8 + sizeof(ids, BinaryDataFormats::sizeof));
		buf.writeLong(timestamp);
		BinaryDataFormats.writeCollection(buf, ids, BinaryDataFormats::writeRawServerId);
		return new AnnounceData(buf.asArray(), timestamp, serverIds);
	}
	// endregion

	public static AnnounceData fromBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		long timestamp = buf.readLong();
		List<RawServerId> rawServerIds = BinaryDataFormats.readList(buf, BinaryDataFormats::readRawServerId);
		return new AnnounceData(bytes, timestamp, new HashSet<>(rawServerIds));
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Set<RawServerId> getServerIds() {
		return serverIds;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AnnounceData that = (AnnounceData) o;

		return timestamp == that.timestamp && serverIds.equals(that.serverIds);
	}

	@Override
	public int hashCode() {
		return 31 * (int) (timestamp ^ (timestamp >>> 32)) + serverIds.hashCode();
	}

	@Override
	public String toString() {
		return "AnnounceData{timestamp=" + timestamp + ", serverIds=" + serverIds + '}';
	}
}
