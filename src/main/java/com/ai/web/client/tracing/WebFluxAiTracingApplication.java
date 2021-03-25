package com.ai.web.client.tracing;

import com.ai.web.client.tracing.model.MemberInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.DefaultUriBuilderFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.util.context.Context;

import java.io.BufferedReader;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@EnableWebFlux
@SpringBootApplication
@EnableConfigurationProperties(WebFluxAiTracingApplication.ServiceHttpConfig.class)
public class WebFluxAiTracingApplication {

	private static final String X_EXTRA_HEADER = "X-EXTRA-ID";
	private static final String EMPTY_STRING = "";

	public static void main(String[] args) {
		new SpringApplicationBuilder()
				.sources(WebFluxAiTracingApplication.class)
				.web(WebApplicationType.REACTIVE)
				.build()
				.run(args);
	}

	@Bean
	public ReactiveWebServerFactory reactiveWebServerFactory() {
		return new NettyReactiveWebServerFactory();
	}

	@RestController
	static class ServiceController {

		private final ObjectMapper objectMapper;
		private final List<MemberInfo> profiles;
		private final WebClient identitiesWebClient;
		private final WebClient infoWebClient;

		ServiceController(
				ObjectMapper objectMapper,
				@Value("classpath:__json/content.json") Resource profiles,
				@Qualifier("identitiesWebClient") WebClient client_1,
				@Qualifier("infoWebClient") WebClient client_2
		) {
			this.objectMapper = objectMapper;
			this.profiles = readEntities(profiles);
			this.identitiesWebClient = client_1;
			this.infoWebClient = client_2;
		}

		@SneakyThrows
		@GetMapping("/v1/members/info")
		public Flux<MemberInfo> getMemberProfiles() {
			return retrieveAll(identitiesWebClient, getMemberIdentitiesServiceURI(), new ParameterizedTypeReference<Long>() { })
					.flatMap(identity ->
							retrieveOne(infoWebClient, getMemberInfoServiceURI(identity), new ParameterizedTypeReference<MemberInfo>() { })
					);
		}

		@GetMapping("/v1/members/identities")
		public Flux<Long> getMemberIdentities() {
			return Flux.fromIterable(profiles)
					.map(MemberInfo::getMemberId);
		}

		@GetMapping("/v1/members/{identity}")
		public Mono<MemberInfo> getMemberInfo(@PathVariable Long identity) {
			return Flux.fromIterable(profiles)
					.filter(Objects::nonNull)
					.filter(it -> identity.equals(it.getMemberId()))
					.single();
		}

		@GetMapping("/v1/members/extra-header")
		public Mono<String> getMemberExtraHeader() {
			return Mono.fromCallable(() -> UUID.randomUUID().toString())
					.subscribeOn(Schedulers.boundedElastic());
		}

		@SneakyThrows
		private URI getMemberIdentitiesServiceURI() {
			return new URI("http://localhost:8080/v1/members/identities");
		}

		@SneakyThrows
		private URI getMemberInfoServiceURI(Long identity) {
			return new URI("http://localhost:8080/v1/members/" + identity);
		}

		@SneakyThrows
		List<MemberInfo> readEntities(Resource entities) {
			String content = getFileContent(entities);

			return objectMapper.readValue(content, new TypeReference<>() {
			});
		}

		@SneakyThrows
		String getFileContent(Resource resource) {
			try (BufferedReader reader = Files.newBufferedReader(resource.getFile().toPath())) {
				return reader != null ? reader.lines().collect(Collectors.joining()) : "";
			}
		}
	}

	@Getter
	@Setter
	@ConfigurationProperties("service.http")
	static class ServiceHttpConfig {
		private Integer connectionTimeout = 5000;
		private Integer readTimeout = 5;
		private Integer writeTimeout = 5;
		private Integer responseCacheSeconds = 3600;
	}


	@Configuration
	static class Config {

		@Bean
		ObjectMapper objectMapper() {
			return applyConfiguration(new ObjectMapper());
		}

		static ObjectMapper applyConfiguration(ObjectMapper objectMapper) {
			return objectMapper
					.registerModule(new Jdk8Module()) .registerModule(new JavaTimeModule())
					.setSerializationInclusion(JsonInclude.Include.NON_NULL)
					.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
					.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
					.registerModules(new SimpleModule() {

						@Override
						public void setupModule(SetupContext context) {
							super.setupModule(context);
							context.insertAnnotationIntrospector(new JacksonAnnotationIntrospector() {

								@Override
								public JsonPOJOBuilder.Value findPOJOBuilderConfig(AnnotatedClass ac) {
									if (ac.hasAnnotation(JsonPOJOBuilder.class)) {
										return super.findPOJOBuilderConfig(ac);
									}
									return new JsonPOJOBuilder.Value("build", "");
								}

							});
						}

					});
		}

		@RequiredArgsConstructor
		static class MemberExtraHeaderFilter implements ExchangeFilterFunction {

			private final WebClient webClient;

			@Override
			public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
				return new MonoWebClientMemberExtraHeader(request, next, webClient);
			}

			@RequiredArgsConstructor
			static class WebClientMemberExtraHeaderSubscriber implements CoreSubscriber<ClientResponse> {

				private final CoreSubscriber<? super ClientResponse> actual;

				@Override
				public Context currentContext() {
					return actual.currentContext();
				}

				@Override
				public void onSubscribe(Subscription subscription) {
					actual.onSubscribe(subscription);
				}

				@Override
				public void onNext(ClientResponse clientResponse) {
					actual.onNext(clientResponse);
				}

				@Override
				public void onError(Throwable t) {
					actual.onError(t);
				}

				@Override
				public void onComplete() {
					actual.onComplete();
				}
			}

			@RequiredArgsConstructor
			static class MonoWebClientMemberExtraHeader extends Mono<ClientResponse> {

				private final ClientRequest request;

				private final ExchangeFunction next;

				private final WebClient webClient;

				@Override
				public void subscribe(CoreSubscriber<? super ClientResponse> actual) {

					retrieveOne(webClient, getMemberExtraHeaderServiceURI(), new ParameterizedTypeReference<String>() { })
							.flatMap(extraHeader -> {
								ClientRequest.Builder builder = ClientRequest.from(request);
								builder.header(X_EXTRA_HEADER, extraHeader);
								return next.exchange(builder.build());
							}).subscribe(new WebClientMemberExtraHeaderSubscriber(actual));
				}

				@SneakyThrows
				private URI getMemberExtraHeaderServiceURI() {
					return new URI("http://localhost:8080/v1/members/extra-header");
				}
			}
		}

		@Bean
		@Qualifier("extraHeaderWebClient")
		WebClient extraHeaderWebClient(
				WebClient.Builder builder,
				ServiceHttpConfig properties,
				ObjectMapper objectMapper
		) {
			return createWebClient(builder, properties, objectMapper);
		}

		@Bean
		@Qualifier("identitiesWebClient")
		WebClient identitiesWebClient(
				WebClient.Builder builder,
				ServiceHttpConfig properties,
				ObjectMapper objectMapper,
				@Qualifier("extraHeaderWebClient") WebClient webClient
		) {
			return createWebClient(builder, properties, objectMapper).mutate().filter(new MemberExtraHeaderFilter(webClient)).build();
		}

		@Bean
		@Qualifier("infoWebClient")
		WebClient infoWebClient(
				WebClient.Builder builder,
				ServiceHttpConfig properties,
				ObjectMapper objectMapper,
				@Qualifier("extraHeaderWebClient") WebClient webClient
		) {
			return createWebClient(builder, properties, objectMapper).mutate().filter(new MemberExtraHeaderFilter(webClient)).build();
		}

		private WebClient createWebClient(
				WebClient.Builder builder,
				ServiceHttpConfig properties,
				ObjectMapper objectMapper
		) {
			HttpClient httpClient = HttpClient.create()
					.tcpConfiguration(client ->
							client
									.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, properties.getConnectionTimeout())
									.doOnConnected(conn -> conn
											.addHandlerLast(new ReadTimeoutHandler(properties.getReadTimeout()))
											.addHandlerLast(new WriteTimeoutHandler(properties.getWriteTimeout()))));

			ExchangeStrategies strategies = ExchangeStrategies.builder()
					.codecs(configurer -> {
						configurer
								.defaultCodecs()
								.jackson2JsonEncoder(new Jackson2JsonEncoder(objectMapper, MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON_UTF8));
						configurer
								.defaultCodecs()
								.jackson2JsonDecoder(new Jackson2JsonDecoder(objectMapper, MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON_UTF8));
					}).build();

			DefaultUriBuilderFactory factory = new DefaultUriBuilderFactory();
			factory.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.URI_COMPONENT);

			return builder
					.clientConnector(new ReactorClientHttpConnector(httpClient))
					.baseUrl("{host}/v1/")
					.uriBuilderFactory(factory)
					.exchangeStrategies(strategies)
					.defaultHeaders(headers -> {
						headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
						headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
					}).build();
		}

	}

	static  <T> Mono<? extends T> retrieveOne(WebClient webClient, URI uri, ParameterizedTypeReference<T> ref) {
		return webClient
				.get()
				.uri(uriBuilder ->
						uriBuilder
								.scheme(getOrDefault(uri.getScheme(), "https"))
								.port(getOrDefault(uri.getPort(), EMPTY_STRING))
								.host(getOrDefault(uri.getHost(), EMPTY_STRING))
								.path(getOrDefault(uri.getPath(), EMPTY_STRING))
								.build()
				)
				.header(X_EXTRA_HEADER, Objects.toString("12345", ""))
				.retrieve()
				.bodyToMono(ref);
	}

	static <T> Flux<? extends T> retrieveAll(WebClient webClient, URI uri, ParameterizedTypeReference<T> ref) {
		return webClient
				.get()
				.uri(uriBuilder ->
						uriBuilder
								.scheme(getOrDefault(uri.getScheme(), "https"))
								.port(getOrDefault(uri.getPort(), EMPTY_STRING))
								.host(getOrDefault(uri.getHost(), EMPTY_STRING))
								.path(getOrDefault(uri.getPath(), EMPTY_STRING))
								.build()
				)
				.header(X_EXTRA_HEADER, Objects.toString("12345", ""))
				.retrieve()
				.bodyToFlux(ref);
	}

	private static String getOrDefault(Object value, String nullValue) {
		return Objects.toString(value, nullValue);
	}

}
