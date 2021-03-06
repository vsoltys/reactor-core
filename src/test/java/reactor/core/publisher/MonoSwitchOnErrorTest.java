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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSwitchOnErrorTest {
/*
	@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(FluxResume.class);
		
		ctb.addRef("source", Flux.never());
		ctb.addRef("nextFactory", (Function<Throwable, Publisher<Object>>)e -> Flux.never());
		
		ctb.test();
	}*/

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .switchOnError(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.just(1)
		    .switchOnError(v -> Mono.just(2))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure")).switchOnError(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorFiltered() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure"))
				.switchOnError(e -> e.getMessage().equals("forced failure"), v -> Mono.just(2))
				.subscribe(ts);

		ts.assertValues(2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void errorMap() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new Exception()).mapError(d -> new RuntimeException("forced" +
				" " +
				"failure"))
		                                    .subscribe(ts);

		ts.assertNoValues()
		  .assertError()
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void errorBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).switchOnError(v -> Mono.just(
				2))
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(2)
		  .assertComplete();
	}

	@Test
	public void nextFactoryThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).switchOnError(v -> {
			throw new RuntimeException("forced failure 2");
		})
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure 2")));
	}

	@Test
	public void nextFactoryReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>error(new RuntimeException("forced failure")).switchOnError(v -> null)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	static final class TestException extends Exception {}

	@Test
	public void mapError() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.mapError(TestException.class, e -> new Exception("test"))
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void otherwiseErrorFilter() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.switchOnError(TestException.class, e -> Mono.just(1))
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void otherwiseErrorUnfilter() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.switchOnError(RuntimeException.class, e -> Mono.just(1))
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyError(TestException.class);
	}

	@Test
	public void otherwiseReturnErrorFilter() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.otherwiseReturn(TestException.class, 1)
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(1)
		            .verifyComplete();
	}


	@Test
	public void otherwiseReturnErrorFilter2() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.otherwiseReturn(TestException.class::isInstance, 1)
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void otherwiseReturnErrorUnfilter() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.otherwiseReturn(RuntimeException.class, 1)
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyError(TestException.class);
	}

	@Test
	public void otherwiseReturnErrorUnfilter2() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.<Integer>error(new TestException())
				.otherwiseReturn(RuntimeException.class::isInstance, 1)
				.subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isTrue())
		            .then(() -> assertThat(mp.isSuccess()).isFalse())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .verifyError(TestException.class);
	}
}
