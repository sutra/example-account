package org.oxerr.example.account.versioned;

import javax.sql.DataSource;

import org.oxerr.example.account.Account;
import org.oxerr.example.account.service.versioned.VersionedAccountService;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

class JMHVersionedAccountService extends VersionedAccountService {

	JMHVersionedAccountService(
		final DataSource dataSource,
		final JedisPool jedisPool,
		final JedisCluster jedisCluster
	) {
		super(dataSource, jedisPool, jedisCluster);
	}

	@Override
	public Account getViaJDBC(long id) {
		return super.getViaJDBC(id);
	}

	@Override
	public void cache(Account account) {
		super.cache(account);
	}

}
