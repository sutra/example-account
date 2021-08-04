package org.oxerr.example.account.database.lock;

import javax.sql.DataSource;

import org.oxerr.example.account.Account;
import org.oxerr.example.account.service.database.lock.DatabaseLockAccountService;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

class JMHDatabaseLockAccountService extends DatabaseLockAccountService {

	JMHDatabaseLockAccountService(
		final DataSource dataSource,
		final JedisPool jedisPool,
		final JedisCluster jedisCluster
	) {
		super(dataSource, jedisPool, jedisCluster);
	}

	@Override
	public Account getViaJDBC(final long id) {
		return super.getViaJDBC(id);
	}

}
