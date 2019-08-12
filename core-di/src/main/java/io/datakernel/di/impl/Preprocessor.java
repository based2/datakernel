package io.datakernel.di.impl;

import io.datakernel.di.core.*;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.di.core.Multibinder.ERROR_ON_DUPLICATE;
import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

/**
 * This class contains a set of utils for working with binding graph trie.
 */
public final class Preprocessor {
	private Preprocessor() {}

	/**
	 * This method converts a trie of binding multimaps, that is provided from the modules,
	 * into a trie of binding maps on which the {@link Injector} would actually operate.
	 * <p>
	 * While doing so it also recursively tries to generate missing dependency bindings
	 * using given {@link BindingGenerator generator} and apply given {@link BindingTransformer} to all of them.
	 *
	 * @see BindingGenerator#combinedGenerator
	 * @see BindingTransformer#combinedTransformer
	 */
	public static Trie<Scope, Map<Key<?>, Binding<?>>> reduce(
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings,
			Map<Key<?>, Multibinder<?>> multibinders,
			BindingTransformer<?> transformer,
			BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> reduced = Trie.leaf(new HashMap<>());
		reduce(UNSCOPED, emptyMap(), bindings, reduced, multibinders, transformer, generator);
		return reduced;
	}

	private static void reduce(
			Scope[] scope, Map<Key<?>, Binding<?>> upper,
			Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings, Trie<Scope, Map<Key<?>, Binding<?>>> reduced,
			Map<Key<?>, Multibinder<?>> multibinders,
			BindingTransformer<?> transformer,
			BindingGenerator<?> generator) {

		Map<Key<?>, Set<Binding<?>>> localBindings = bindings.get();

		localBindings.forEach((key, bindingSet) -> resolve(upper, localBindings, reduced.get(), scope, key, bindingSet, multibinders, transformer, generator));

		Map<Key<?>, Binding<?>> nextUpper = override(upper, reduced.get());

		bindings.getChildren().forEach((subScope, subLocalBindings) ->
				reduce(next(scope, subScope), nextUpper, subLocalBindings, reduced.computeIfAbsent(subScope, $ -> new HashMap<>()), multibinders, transformer, generator));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private static Binding<?> resolve(
			Map<Key<?>, Binding<?>> upper, Map<Key<?>, Set<Binding<?>>> localBindings, Map<Key<?>, Binding<?>> resolved,
			Scope[] scope, Key<?> key, @Nullable Set<Binding<?>> bindingSet,
			Map<Key<?>, Multibinder<?>> multibinders, BindingTransformer<?> transformer, BindingGenerator<?> generator) {

		// shortest path - if it was already resolved, just return it (also serves as a visited set so graph loops dont cause infinite recursion)
		Binding<?> reduced = resolved.get(key);
		if (reduced != null) {
			return reduced;
		}

		BindingLocator recursiveLocator = new BindingLocator() {
			@Override
			@Nullable
			public <T> Binding<T> get(Key<T> key) {
				return (Binding<T>) resolve(upper, localBindings, resolved, scope, key, localBindings.get(key), multibinders, transformer, generator);
			}
		};

		// if it was explicitly bound
		if (bindingSet != null) {
			switch (bindingSet.size()) {
				case 0:
					// try to recursively generate a requested binding
					reduced = ((BindingGenerator<Object>) generator).generate(recursiveLocator, scope, (Key<Object>) key);
					// when empty set is explicitly provided it means that a plain `bind(...)` request was used, and we fail fast then
					if (reduced == null) {
						throw new DIException("Refused to generate an explicitly requested binding for key " + key.getDisplayString());
					}
					break;
				case 1:
					reduced = bindingSet.iterator().next();
					break;
				default:
					reduced = ((Multibinder) multibinders.getOrDefault(key, ERROR_ON_DUPLICATE)).multibind(bindingSet);
			}
		} else { // or if it was never bound
			// first check if it was already resolved in upper scope
			reduced = upper.get(key);
			if (reduced != null) {
				return reduced;
			}
			// try to generate it
			reduced = ((BindingGenerator<Object>) generator).generate(recursiveLocator, scope, (Key<Object>) key);

			// if it was not generated then it's simply unsatisfied and later will be checked
			if (reduced == null) {
				return null;
			}
		}

		// transform it (once!)
		reduced = ((BindingTransformer) transformer).transform(recursiveLocator, scope, key, reduced);

		// and store it as resolved (also mark as visited)
		resolved.put(key, reduced);

		// and then recursively walk over its dependencies (so this is a recursive dfs after all)
		for (Dependency d : reduced.getDependencies()) {
			Key<?> dkey = d.getKey();
			resolve(upper, localBindings, resolved, scope, dkey, localBindings.get(dkey), multibinders, transformer, generator);
		}

		return reduced;
	}

	/**
	 * A method that checks binding graph trie completeness, meaning that no binding references a key that is not present
	 * at same or lower level of the trie, and that there is no cycles in each scope (cycles between scopes are not possible).
	 * <p>
	 * It doesn't throw an error if and only if this graph is complete and being complete means that each bindings dependencies should
	 * have a respective binding in the local or upper scope, and that there is no cyclic dependencies.
	 */
	public static void check(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		checkUnsatisfied(UNSCOPED, known, bindings);
		checkCycles(UNSCOPED, bindings);
	}

	private static void checkUnsatisfied(Scope[] scope, Set<Key<?>> upperKnown, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {

		class DependencyToBinding {
			final Key<?> dependency;
			final Entry<Key<?>, Binding<?>> keybinding;

			public DependencyToBinding(Key<?> dependency, Entry<Key<?>, Binding<?>> keybinding) {
				this.dependency = dependency;
				this.keybinding = keybinding;
			}
		}

		Set<Key<?>> known = union(upperKnown, bindings.get().keySet());

		Map<? extends Key<?>, Set<Entry<Key<?>, Binding<?>>>> unsatisfied = bindings.get().entrySet().stream()
				.flatMap(e -> e.getValue().getDependencies().stream()
						.filter(dependency -> dependency.isRequired() && !known.contains(dependency.getKey()))
						.map(dependency -> new DependencyToBinding(dependency.getKey(), e)))

				.collect(toMultimap(dtb -> dtb.dependency, dtb -> dtb.keybinding));

		if (!unsatisfied.isEmpty()) {
			throw new DIException(unsatisfied.entrySet().stream()
					.map(entry -> {
						Key<?> required = entry.getKey();
						String displayString = required.getDisplayString();
						return entry.getValue().stream()
								.map(binding -> {
									Key<?> key = binding.getKey();
									String displayAndLocation = key.getDisplayString() + " " + Utils.getLocation(binding.getValue());
									Class<?> rawType = key.getRawType();
									if (Modifier.isStatic(rawType.getModifiers()) || !required.getRawType().equals(rawType.getEnclosingClass())) {
										return displayAndLocation;
									}
									String indent = new String(new char[Utils.getKeyDisplayCenter(key) + 2]).replace('\0', ' ');
									return displayAndLocation + "\n\t\t" + indent + "^- this is a non-static inner class with implicit dependency on its enclosing class";
								})
								.collect(joining("\n\t\t- ", "\tkey " + displayString + " required to make:\n\t\t- ", ""));
					})
					.collect(joining("\n", "Unsatisfied dependencies detected" + (scope.length != 0 ? " in scope " + getScopeDisplayString(scope) : "") + ":\n", "\n")));
		}

		bindings.getChildren().forEach((subScope, subBindings) -> checkUnsatisfied(next(scope, subScope), known, subBindings));
	}

	private static void checkCycles(Scope[] scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		// since no cycles are possible between scopes,
		// we just run a simple dfs that ignores unsatisfied
		// dependencies for each scope independently
		Set<Key<?>[]> cycles = collectCycles(bindings.get());

		if (!cycles.isEmpty()) {
			throw new DIException(cycles.stream()
					.map(Utils::drawCycle)
					.collect(joining("\n\n", "Cyclic dependencies detected" + (scope.length != 0 ? " in scope " + getScopeDisplayString(scope) : "") + ":\n\n", "\n")));
		}

		bindings.getChildren()
				.forEach((subScope, subBindings) ->
						checkCycles(next(scope, subScope), subBindings));
	}

	/**
	 * This method performs a simple recursive DFS on given binding graph and returns all found cycles.
	 * <p>
	 * Unsatisfied dependencies are ignored.
	 */
	public static Set<Key<?>[]> collectCycles(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		// the DAG is not necessarily connected, so we go through any possibly disconnected part
		for (Key<?> key : bindings.keySet()) {
			if (!visited.contains(key)) {
				collectCycles(bindings, visited, visiting, cycles, key);
			}
		}
		return cycles;
	}

	private static void collectCycles(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, Set<Key<?>[]> cycles, Key<?> key) {
		Binding<?> binding = bindings.get(key);
		if (binding == null) {
			// just ignore unsatisfied dependencies as if they never existed
			// (they may be unsatisfied and be checked later by unsatisfied dependency check or they may just reference some upper scope)
			visited.add(key); // add to visited as a tiny optimization
			return;
		}
		// standard dfs with visited (black) and visiting (grey) sets
		if (visiting.add(key)) {
			for (Dependency dependency : binding.getDependencies()) {
				if (!visited.contains(dependency.getKey())) {
					collectCycles(bindings, visited, visiting, cycles, dependency.getKey());
				}
			}
			visiting.remove(key);
			visited.add(key);
			return;
		}

		// so at this point visiting set looks something like a -> b -> c -> d -> e -> g (-> c),
		// and in the code below we just get d -> e -> g -> c out of it

		Iterator<Key<?>> backtracked = visiting.iterator();
		int skipped = 0;

		// no .hasNext check since the the set must contain the key because above .add call returned false
		while (!backtracked.next().equals(key)) {
			skipped++;
		}
		Key<?>[] cycle = new Key[visiting.size() - skipped];
		for (int i = 0; i < cycle.length - 1; i++) {
			// no .hasNext check either because this happens exactly (size - previous .next calls - 1) times
			cycle[i] = backtracked.next();
		}
		// no key was added to the set because it already was there
		// and that one was consumed by the skipping while loop above
		// so we just add it manually at the end
		cycle[cycle.length - 1] = key;
		cycles.add(cycle);
	}
}
