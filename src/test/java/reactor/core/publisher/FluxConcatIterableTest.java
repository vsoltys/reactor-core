/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.subscriber.AssertSubscriber;

public class FluxConcatIterableTest {

	@Test(expected = NullPointerException.class)
	public void arrayNull() {
		Flux.concat((Iterable<? extends Publisher<?>>)null);
	}

	final Publisher<Integer> source = Flux.range(1, 3);

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList(source, source, source)).subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.concat(Arrays.asList(source, source, source)).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 3, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void oneSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList(source, null, source)).subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void singleSourceIsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.concat(Arrays.asList((Publisher<Integer>) null)).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

}
