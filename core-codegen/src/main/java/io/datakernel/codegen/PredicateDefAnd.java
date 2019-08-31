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

package io.datakernel.codegen;

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static org.objectweb.asm.Type.BOOLEAN_TYPE;

/**
 * Defines methods for using logical 'and' for boolean type
 */
final class PredicateDefAnd implements PredicateDef {
	private final List<PredicateDef> predicates;

	PredicateDefAnd(List<PredicateDef> predicates) {
		this.predicates = predicates;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		Label exit = new Label();
		Label labelFalse = new Label();
		for (PredicateDef predicate : predicates) {
			Type type = predicate.load(ctx);
			assert type == BOOLEAN_TYPE;
			g.ifZCmp(GeneratorAdapter.EQ, labelFalse);
		}
		g.push(true);
		g.goTo(exit);

		g.mark(labelFalse);
		g.push(false);

		g.mark(exit);
		return BOOLEAN_TYPE;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PredicateDefAnd that = (PredicateDefAnd) o;
		return predicates.equals(that.predicates);
	}

	@Override
	public int hashCode() {
		return predicates.hashCode();
	}
}
