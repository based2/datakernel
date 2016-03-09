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

package io.datakernel.config;

import com.google.common.reflect.TypeToken;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.Splitter;
import org.joda.time.Period;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static io.datakernel.util.Preconditions.*;
import static java.util.Arrays.asList;

@SuppressWarnings("unchecked")
public final class ConfigTree implements Config {
	private Map<TypeToken, ConfigConverter> converters;

	private static final Splitter SPLITTER = Splitter.on('.');

	private final Map<String, ConfigTree> children = new LinkedHashMap<>();

	private ConfigTree parent;
	private String value;
	private String defaultValue;

	private boolean accessed;
	private boolean modified;

	// creators
	private ConfigTree() {
		this(null);
	}

	private ConfigTree(ConfigTree parent) {
		this.parent = parent;
		this.converters = parent == null ? defaultConverters() : parent.converters;
	}

	private static Map<TypeToken, ConfigConverter> defaultConverters() {
		Map<TypeToken, ConfigConverter> defaultConverters = new HashMap<>();

		defaultConverters.put(TypeToken.of(boolean.class), ConfigConverters.ofBoolean());
		defaultConverters.put(TypeToken.of(Boolean.class), ConfigConverters.ofBoolean());

		defaultConverters.put(TypeToken.of(int.class), ConfigConverters.ofInteger());
		defaultConverters.put(TypeToken.of(Integer.class), ConfigConverters.ofInteger());

		defaultConverters.put(TypeToken.of(long.class), ConfigConverters.ofLong());
		defaultConverters.put(TypeToken.of(Long.class), ConfigConverters.ofLong());

		defaultConverters.put(TypeToken.of(double.class), ConfigConverters.ofDouble());
		defaultConverters.put(TypeToken.of(Double.class), ConfigConverters.ofDouble());

		defaultConverters.put(TypeToken.of(String.class), ConfigConverters.ofString());

		defaultConverters.put(TypeToken.of(DatagramSocketSettings.class), ConfigConverters.ofDatagramSocketSettings());
		defaultConverters.put(TypeToken.of(InetSocketAddress.class), ConfigConverters.ofInetSocketAddress());
		defaultConverters.put(TypeToken.of(MemSize.class), ConfigConverters.ofMemSize());
		defaultConverters.put(TypeToken.of(Period.class), ConfigConverters.ofPeriod());
		defaultConverters.put(TypeToken.of(ServerSocketSettings.class), ConfigConverters.ofServerSocketSettings());
		defaultConverters.put(TypeToken.of(SocketSettings.class), ConfigConverters.ofSocketSettings());

		return defaultConverters;
	}

	public static ConfigTree newInstance() {
		return newInstance(null);
	}

	public static ConfigTree newInstance(ConfigTree parent) {
		return new ConfigTree(parent);
	}

	public <T> ConfigTree registerConverter(TypeToken<T> type, ConfigConverter<T> converter) {
		converters.put(type, converter);
		return this;
	}

	public <T> ConfigTree registerConverter(Class<T> type, ConfigConverter<T> converter) {
		return registerConverter(TypeToken.of(type), converter);
	}

	// api
	@Override
	synchronized public Config getChild(String path) {
		final ConfigTree config = ensureChild(path);
		config.accessed = true;
		return config;
	}

	@Override
	synchronized public Map<String, Config> getChildren() {
		return (Map) Collections.unmodifiableMap(children);
	}

	@Override
	public <T> T get(Class<T> type) {
		return get(TypeToken.of(type));
	}

	@Override
	synchronized public <T> T get(TypeToken<T> type) {
		ConfigConverter<T> converter = converters.get(type);
		checkNotNull(converter, "Missing config converter for type: %s", type);
		return converter.get(this);
	}

	@Override
	synchronized public <T> T get(Class<T> type, T defaultValue) {
		return get(TypeToken.of(type), defaultValue);
	}

	@Override
	synchronized public <T> T get(TypeToken<T> type, T defaultValue) {
		checkNotNull(defaultValue);
		ConfigConverter<T> converter = converters.get(type);
		checkNotNull(converter);
		return converter.get(this, defaultValue);
	}

	@Override
	synchronized public <T> T get(String path, Class<T> type) {
		return get(path, TypeToken.of(type));
	}

	@Override
	synchronized public <T> T get(String path, TypeToken<T> type) {
		return ensureChild(path).get(type);
	}

	@Override
	synchronized public <T> T get(String path, Class<T> type, T defaultValue) {
		return get(path, TypeToken.of(type), defaultValue);
	}

	@Override
	synchronized public <T> T get(String path, TypeToken<T> type, T defaultValue) {
		return ensureChild(path).get(type, defaultValue);

	}

	// common
	@Override
	public String toString() {
		return getKey();
	}

	@Override
	public synchronized boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ConfigTree config = (ConfigTree) o;
		return Objects.equals(this.children, config.children) &&
				Objects.equals(this.value, config.value);
	}

	// util
	synchronized public boolean hasSection(String path) {
		ConfigTree child = ensureChild(path);
		child.accessed = true;
		return child.children.size() > 0 && child.value == null;
	}

	synchronized public boolean hasValue(String path) {
		ConfigTree child = ensureChild(path);
		child.accessed = true;
		return child.value != null;
	}

	synchronized public boolean isAccessed() {
		return accessed;
	}

	synchronized public boolean isModified() {
		return modified;
	}

	public static ConfigTree union(ConfigTree... configs) {
		return union(asList(configs));
	}

	public static ConfigTree union(Collection<ConfigTree> configs) {
		if (configs.size() == 1)
			return configs.iterator().next();

		Map<TypeToken, ConfigConverter> map = new HashMap<>();
		for (ConfigTree config : configs) {
			for (Map.Entry<TypeToken, ConfigConverter> entry : config.converters.entrySet()) {
				ConfigConverter converter = map.get(entry.getKey());
				if (converter != null) {
					checkState(converter == entry.getValue(), "Duplicate converter for type: {}", entry.getKey());
				} else {
					map.put(entry.getKey(), entry.getValue());
				}
			}
		}

		return doUnion(null, configs, map);
	}

	private static ConfigTree doUnion(ConfigTree parent, Collection<ConfigTree> configs, Map<TypeToken, ConfigConverter> map) {
		ConfigTree result = new ConfigTree(parent);
		result.converters = map;
		Map<String, List<ConfigTree>> childrenList = new LinkedHashMap<>();

		for (ConfigTree config : configs) {
			if (config.value != null) {
				if (result.value != null) {
					throw new IllegalStateException("Multiple values for " + config.getKey());
				}
				result.value = config.value;
			}
			for (String key : config.children.keySet()) {
				ConfigTree child = config.children.get(key);
				child.converters = map;
				List<ConfigTree> list = childrenList.get(key);
				if (list == null) {
					list = new ArrayList<>();
					childrenList.put(key, list);
				}
				list.add(child);
			}
		}

		for (String key : childrenList.keySet()) {
			List<ConfigTree> childConfigs = childrenList.get(key);
			ConfigTree joined = doUnion(result, childConfigs, map);
			result.children.put(key, joined);
		}

		return result;
	}

	public static ConfigTree clone(ConfigTree config) {
		return clone(config, null);
	}

	private static ConfigTree clone(ConfigTree config, ConfigTree parent) {
		ConfigTree clone = new ConfigTree(parent);
		for (Map.Entry<String, ConfigTree> entry : config.children.entrySet()) {
			ConfigTree clonedConfig = clone(entry.getValue(), clone);
			clone.children.put(entry.getKey(), clonedConfig);
		}
		clone.accessed = config.accessed;
		clone.defaultValue = config.defaultValue;
		clone.value = config.value;
		return clone;
	}

	synchronized public String getKey() {
		if (parent == null)
			return "";
		for (String childKey : parent.children.keySet()) {
			Config child = parent.children.get(childKey);
			if (child == this) {
				String childRootKey = parent.getKey();
				return childRootKey.isEmpty() ? childKey : childRootKey + "." + childKey;
			}
		}
		throw new IllegalStateException();
	}

	synchronized public String get() {
		accessed = true;
		return value;
	}

	synchronized public String get(String defaultValue) {
		if (this.defaultValue != null) {
			if (!this.defaultValue.equals(defaultValue)) {
				throw new IllegalArgumentException("Key '" + getKey() + "': Previous default value '" + this.defaultValue + "' differs from new default value '"
						+ defaultValue + "'");
			}
		} else {
			this.defaultValue = defaultValue;
		}
		String result = get();
		return result != null ? result : defaultValue;
	}

	synchronized public void set(String value) {
		modified = true;
		this.value = value;
	}

	synchronized public void set(String path, String value) {
		ensureChild(path).set(value);
	}

	synchronized public <T> void set(TypeToken<T> type, T value) {
		ConfigConverter<T> converter = converters.get(type);
		checkNotNull(converter);
		checkNotNull(value);
		converter.set(this, value);
	}

	synchronized public <T> void set(String path, TypeToken<T> type, T value) {
		ensureChild(path).set(type, value);
	}

	public void saveToPropertiesFile(Path path) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
			saveToPropertiesFile("", writer);
		}
	}

	synchronized private boolean saveToPropertiesFile(String prefix, Writer writer) throws IOException {
		boolean rootLevel = prefix.isEmpty();
		StringBuilder sb = new StringBuilder();
		if (value != null || defaultValue != null) {
			if (!accessed) {
				assert defaultValue == null;
				sb.append("# Unused: ");
				sb.append(propertiesFileEncode(prefix, true));
				sb.append(" = ");
				sb.append(propertiesFileEncode(value, false));
			} else {
				if (value != null && !value.equals(defaultValue)) {
					sb.append(propertiesFileEncode(prefix, true));
					sb.append(" = ");
					sb.append(propertiesFileEncode(value, false));
				} else { // defaultValue != null
					sb.append("#");
					sb.append(propertiesFileEncode(prefix, true));
					sb.append(" = ");
					if (defaultValue != null) {
						sb.append(propertiesFileEncode(defaultValue, false));
					}
				}
			}
		}
		boolean saved = false;
		String line = sb.toString();
		if (!line.isEmpty()) {
			writer.write(line + '\n');
			saved = true;
		}
		for (String key : children.keySet()) {
			ConfigTree child = children.get(key);
			boolean savedByChild = child.saveToPropertiesFile(rootLevel ? key : (prefix + "." + key), writer);
			if (rootLevel && savedByChild) {
				writer.write('\n');
			}
			saved |= savedByChild;
		}
		return saved;
	}

	private static String propertiesFileEncode(String string, boolean escapeKey) {
		StringBuilder sb = new StringBuilder(string.length() * 2);

		for (int i = 0; i < string.length(); i++) {
			char c = string.charAt(i);
			if ((c > 61) && (c < 127)) {
				if (c == '\\') {
					sb.append('\\');
					sb.append('\\');
					continue;
				}
				sb.append(c);
				continue;
			}
			switch (c) {
				case ' ':
					if (i == 0 || escapeKey)
						sb.append('\\');
					sb.append(' ');
					break;
				case '\t':
					sb.append('\\');
					sb.append('t');
					break;
				case '\n':
					sb.append('\\');
					sb.append('n');
					break;
				case '\r':
					sb.append('\\');
					sb.append('r');
					break;
				case '\f':
					sb.append('\\');
					sb.append('f');
					break;
				case '=':
				case ':':
				case '#':
				case '!':
					sb.append('\\');
					sb.append(c);
					break;
				default:
					sb.append(c);
			}
		}
		return sb.toString();
	}

	public ConfigTree ensureChild(String path) {
		checkArgument(!path.isEmpty(), "Path must not be empty");
		ConfigTree result = this;
		for (String key : SPLITTER.splitToList(path)) {
			checkState(!key.isEmpty(), "Child path must not be empty: %s", path);
			ConfigTree child = result.children.get(key);
			if (child == null) {
				child = new ConfigTree(result);
				result.children.put(key, child);
			}
			result = child;
		}
		return result;
	}

	public synchronized boolean contains(ConfigTree config) {
		if (this == config)
			return true;
		if (!Objects.equals(this.value, config.value))
			return false;
		for (Map.Entry<String, ConfigTree> entry : config.children.entrySet()) {
			ConfigTree childrenConfig = this.children.get(entry.getKey());
			if (childrenConfig == null || !childrenConfig.contains(entry.getValue()))
				return false;
		}
		return true;
	}

	public synchronized void removeUnused() {
		for (Map.Entry<String, ConfigTree> entry : new ArrayList<>(children.entrySet())) {
			final ConfigTree value = entry.getValue();
			if (value.children.isEmpty()) {
				if (!value.accessed)
					children.remove(entry.getKey());
			} else {
				value.removeUnused();
			}
		}
	}

	public synchronized boolean update(ConfigTree config) {
		this.converters.putAll(config.converters);
		return doUpdate(config);
	}

	private boolean doUpdate(ConfigTree config) {
		checkNotNull(config);
		boolean updated = false;
		if (!Objects.equals(this.value, config.value)) {
			this.value = config.value;
			updated = true;
		}

		for (Map.Entry<String, ConfigTree> entry : config.children.entrySet()) {
			String key = entry.getKey();
			ConfigTree value = entry.getValue();
			if (!this.children.containsKey(key)) {
				ConfigTree conf = clone(value);
				conf.converters = this.converters;
				this.children.put(key, conf);
				updated = true;
			} else {
				if (this.children.get(key).update(value)) {
					updated = true;
				}
			}
		}
		return updated;
	}

	public synchronized ConfigTree diff(ConfigTree config) {
		checkNotNull(config);

		ConfigTree diff = new ConfigTree();
		if (!Objects.equals(this.value, config.value)) {
			diff.value = config.value;
		}

		for (Map.Entry<String, ConfigTree> entry : config.children.entrySet()) {
			String key = entry.getKey();
			ConfigTree value = entry.getValue();
			if (!this.children.containsKey(key)) {
				diff.children.put(key, clone(value));
			} else {
				ConfigTree subDiff = this.children.get(key).diff(value);
				if (subDiff != null)
					diff.children.put(key, subDiff);
			}
		}
		if (diff.value == null && diff.children.isEmpty())
			return null;
		return diff;
	}
}
