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
 * CREATE DATABASE example;
 * CREATE USER 'example'@'localhost' IDENTIFIED BY 'G9&zqkNv3*XA8i2#';
 * GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, DROP, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES ON example.* TO 'example'@'localhost';
 *
 * USE example;
 * CREATE TABLE `account` (
 *   `id` bigint unsigned NOT NULL,
 *   `available` bigint NOT NULL,
 *   `version` bigint unsigned NOT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 *
 * -- SHOW VARIABLES LIKE "max_connections";
 * -- SET GLOBAL max_connections = 500;
 * -- SHOW STATUS WHERE `variable_name` = 'Threads_connected';
 * -- SHOW PROCESSLIST;
 */
@State(Scope.Benchmark)
public class MySQLAccountServiceBenchmark implements AutoCloseable {

	private final Logger log = LogManager.getLogger(MySQLAccountServiceBenchmark.class);

	private final AccountService accountService;

	private final HikariDataSource dataSource;

	private final JedisPool jedisPool;

	public MySQLAccountServiceBenchmark() {
		final var poolSize = 100;
		final var minIdle = 100;

		var jdbcUrl = "jdbc:mysql://localhost/example";

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
