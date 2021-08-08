package org.oxerr.example.account.versioned;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.oxerr.example.account.Account;
import org.oxerr.example.account.JedisClusterFactory;
import org.oxerr.example.account.JedisPoolFactory;
import org.oxerr.example.account.MySQLDataSourceFactory;

import com.zaxxer.hikari.HikariDataSource;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

@State(Scope.Benchmark)
public class PostgreSQLAccountServiceBenchmark implements AutoCloseable {

	private final JMHVersionedAccountService accountService;

	private final HikariDataSource dataSource;

	private JedisPool jedisPool;

	private JedisCluster jedisCluster;

	public PostgreSQLAccountServiceBenchmark() {
		this.dataSource = new MySQLDataSourceFactory().getDataSource();

		try {
			this.jedisCluster = new JedisClusterFactory().getJedisCluster();
		} catch (redis.clients.jedis.exceptions.JedisDataException e) {
			this.jedisPool = new JedisPoolFactory().getJedisPool();
		}

		this.accountService = new JMHVersionedAccountService(dataSource, jedisPool, jedisCluster);

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
	public void testGetViaJDBC() {
		this.accountService.getViaJDBC(1L);
	}

	@Benchmark
	public void testAddAmount() {
		this.accountService.addAmount(1L, 1);
	}

	@Benchmark
	public void testCache() {
		this.accountService.cache(new Account(1L, 0, 0));
	}

	@Override
	public void close() {
		this.dataSource.close();
		this.jedisPool.close();
		this.jedisCluster.close();
	}

}
