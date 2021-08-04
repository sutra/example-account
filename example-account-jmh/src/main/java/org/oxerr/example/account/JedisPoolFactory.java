package org.oxerr.example.account;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolFactory {

	public JedisPool getJedisPool() {
		final var poolSize = 100;
		final var minIdle = 100;

		var host = "localhost";
		var port = 6379;

		var cfg = new JedisPoolConfig();
		cfg.setMaxTotal(poolSize);
		cfg.setMinIdle(minIdle);

		return new JedisPool(cfg, host, port);
	}

}
