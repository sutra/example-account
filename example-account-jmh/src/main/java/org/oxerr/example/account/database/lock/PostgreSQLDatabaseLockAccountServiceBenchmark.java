package org.oxerr.example.account.database.lock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.oxerr.example.account.JedisClusterFactory;
import org.oxerr.example.account.JedisPoolFactory;
import org.oxerr.example.account.MySQLDataSourceFactory;

import com.zaxxer.hikari.HikariDataSource;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

@State(Scope.Benchmark)
public class PostgreSQLDatabaseLockAccountServiceBenchmark implements AutoCloseable {

	private final JMHDatabaseLockAccountService accountService;

	private final HikariDataSource dataSource;

	private JedisPool jedisPool;

	private JedisCluster jedisCluster;

	public PostgreSQLDatabaseLockAccountServiceBenchmark() {
		this.dataSource = new MySQLDataSourceFactory().getDataSource();

		try {
			this.jedisCluster = new JedisClusterFactory().getJedisCluster();
		} catch (redis.clients.jedis.exceptions.JedisDataException e) {
			this.jedisPool = new JedisPoolFactory().getJedisPool();
		}

		this.accountService = new JMHDatabaseLockAccountService(dataSource, jedisPool, jedisCluster);

		// Initialize
		final long count = 1_000_000;
		if (this.accountService.count() < count) {
			this.accountService.newAccounts(1, count, Short.MAX_VALUE);
		}
	}

	@Benchmark
	public void testGetViaJDBC() {
		this.accountService.getViaJDBC(1L);
	}

	@Benchmark
	public void testAddAmount() {
		this.accountService.addAmount(1L, 1);
	}

	@Override
	public void close() {
		this.dataSource.close();
		this.jedisPool.close();
		this.jedisCluster.close();
	}

	public static void main(String[] args) {
		Logger log = LogManager.getLogger(PostgreSQLDatabaseLockAccountServiceBenchmark.class);

		try (final var benchmark = new PostgreSQLDatabaseLockAccountServiceBenchmark()) {
			var account = benchmark.accountService.addAmount(1, 1);
			log.info("account: {}", account);
		}
	}

}
