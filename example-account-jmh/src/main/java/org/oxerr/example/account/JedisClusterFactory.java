package org.oxerr.example.account;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class JedisClusterFactory {

	public JedisCluster getJedisCluster() {
		final var poolSize = 100;
		final var minIdle = 100;

		var host = "localhost";
		var port = 6379;

		var cfg = new JedisPoolConfig();
		cfg.setMaxTotal(poolSize);
		cfg.setMinIdle(minIdle);

		return new JedisCluster(new HostAndPort(host, port), cfg);
	}

}
