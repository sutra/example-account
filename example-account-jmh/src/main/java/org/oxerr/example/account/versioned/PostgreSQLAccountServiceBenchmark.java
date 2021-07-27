package org.oxerr.example.account.versioned;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * $ psql -U postgres
 *
 * CREATE USER example WITH ENCRYPTED PASSWORD 'G9&zqkNv3*XA8i2#';
 * CREATE DATABASE example OWNER example;
 * \q
 *
 * $ psql -U example
 *
 * CREATE TABLE account (
 * 	id int8 NOT NULL,
 * 	available int8 NOT NULL,
 * 	"version" int8 NOT NULL,
 * 	CONSTRAINT account_pk PRIMARY KEY (id)
 * );
 *
 * -- SHOW max_connections;
 */
@State(Scope.Benchmark)
public class PostgreSQLAccountServiceBenchmark implements AutoCloseable {

	private final Logger log = LogManager.getLogger(PostgreSQLAccountServiceBenchmark.class);

	private final AccountService accountService;

	private final HikariDataSource dataSource;

	private final JedisPool jedisPool;

	public PostgreSQLAccountServiceBenchmark() {
		final var poolSize = 100;
		final var minIdle = 100;

		var jdbcUrl = "jdbc:postgresql://localhost/example";

		var hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(jdbcUrl);
		hikariConfig.setUsername("example");
		hikariConfig.setPassword("G9&zqkNv3*XA8i2#");
		hikariConfig.setMaximumPoolSize(poolSize);
		hikariConfig.setMinimumIdle(minIdle);
		log.trace("hikariConfig: {}", () -> ToStringBuilder.reflectionToString(hikariConfig, ToStringStyle.MULTI_LINE_STYLE));

		this.dataSource = new HikariDataSource(hikariConfig);

		var host = "localhost";
		var port = 6379;

		var cfg = new JedisPoolConfig();
		cfg.setMaxTotal(poolSize);
		cfg.setMinIdle(minIdle);

		this.jedisPool = new JedisPool(cfg, host, port);

		this.accountService = new AccountService(dataSource, jedisPool);

		// Initialize
		final long count = 1_000_000;
		if (this.accountService.count() < count) {
			this.accountService.newAccounts(1, count, Short.MAX_VALUE);
		}
	}

	@Benchmark
	public void testGet() {
		this.accountService.get(1L);
	}

	@Benchmark
	public void testGetViaJdbc() {
		this.accountService.getViaJdbc(1L);
	}

	@Benchmark
	public void testAddAmount() {
		this.accountService.addAmount(1L, 1);
	}

	@Benchmark
	public void testCache() {
		this.accountService.cache(1L, 0, 0);
	}

	@Override
	public void close() throws Exception {
		this.dataSource.close();
		this.jedisPool.close();
	}

}
