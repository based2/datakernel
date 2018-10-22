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

package io.global.common;

import org.spongycastle.crypto.CryptoException;

import java.util.Arrays;

import static io.global.common.CryptoUtils.decryptECIES;
import static io.global.common.CryptoUtils.encryptECIES;

public final class AsymmetricEncryptedData {
	private final byte[] bytes;

	private AsymmetricEncryptedData(byte[] bytes) {
		this.bytes = bytes;
	}

	public static AsymmetricEncryptedData of(byte[] encryptedBytes) {
		return new AsymmetricEncryptedData(encryptedBytes);
	}

	public static AsymmetricEncryptedData of(ByteArrayIdentity item, PubKey pubKey) {
		return new AsymmetricEncryptedData(encryptECIES(item.toBytes(), pubKey.getEcPublicKey()));
	}

	public byte[] decrypt(PrivKey privKey) {
		try {
			return decryptECIES(bytes, privKey.getEcPrivateKey());
		} catch (CryptoException e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AsymmetricEncryptedData that = (AsymmetricEncryptedData) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public String toString() {
		return "AsymmetricEncryptedData@" + Integer.toHexString(hashCode());
	}
}
