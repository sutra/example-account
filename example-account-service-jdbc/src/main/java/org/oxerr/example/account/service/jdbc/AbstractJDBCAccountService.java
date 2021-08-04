package org.oxerr.example.account.service.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.oxerr.example.account.Account;
import org.oxerr.example.account.service.AccountService;
import org.springframework.dao.TransientDataAccessResourceException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ZAddParams;

public abstract class AbstractJDBCAccountService implements AccountService {

	private static final long EXPIRATION_SECONDS = Duration.ofDays(1).toMinutes() * 60;

	private final Logger log = LogManager.getLogger(AbstractJDBCAccountService.class);

	protected final DataSource dataSource;

	protected final String selectSql;

	private final JedisPool jedisPool;

	private final JedisCluster jedisCluster;

	private final ObjectMapper objectMapper;

	protected AbstractJDBCAccountService(
		final DataSource dataSource,
		final JedisPool jedisPool,
		final JedisCluster jedisCluster
	) {
		this.dataSource = dataSource;
		this.jedisPool = jedisPool;
		this.jedisCluster = jedisCluster;
		this.objectMapper = new ObjectMapper();
		this.selectSql = "select id, available, version from account where id = ?";
	}

	public long count() {
		final long count;
		final var countSql = "select count(*) from account";
		try (
			final var conn = this.dataSource.getConnection();
			final var countPstmt = conn.prepareStatement(countSql);
			final var rs = countPstmt.executeQuery();
		) {
			if (rs.next()) {
				count = rs.getLong(1);
			} else {
				count = 0L;
			}
		} catch (SQLException e) {
			throw new TransientDataAccessResourceException(e.getMessage(), e);
		}

		return count;
	}

	public void newAccounts(final long from, final long to, final long batchSize) {
		final var insertSql = "insert into account(id, available, version) values(?, ?, ?)";
		try (
			final var conn = this.dataSource.getConnection();
			final var insert = conn.prepareStatement(insertSql);
		) {
			for (long id = from; id <= to; id++) {
				log.trace("id: {}", id);
				insert.setLong(1, id);
				insert.setLong(2, 0);
				insert.setLong(3, 0);
				insert.addBatch();

				if (id % batchSize == 0) {
					insert.executeLargeBatch();
				}
			}
			insert.executeLargeBatch();
		} catch (SQLException e) {
			log.warn(e.getMessage());
		}
	}

	@Override
	public Account get(final long id) {
		log.trace("Getting {}", id);

		return this.getViaJedis(id).orElseGet(() -> {
			final var account = this.getViaJDBC(id);
			this.cache(account);
			return account;
		});
	}

	protected Account getViaJDBC(final long id) {
		log.trace("Getting via JDBC: {}", id);

		try {
			return this.getViaJDBCInternal(id);
		} catch (SQLException e) {
			throw new TransientDataAccessResourceException(e.getMessage(), e);
		}
	}

	private Account getViaJDBCInternal(final long id) throws SQLException {
		final Account account;

		try (
			final var conn = this.dataSource.getConnection();
			final var select = conn.prepareStatement(this.selectSql);
		) {
			select.setLong(1, id);

			try (final ResultSet rs = select.executeQuery()) {
				if (rs.next()) {
					account = new Account(id, rs.getLong("available"), rs.getLong("version"));
				} else {
					throw new IllegalArgumentException(String.format("No such account with ID %d.", id));
				}
			}
		}

		return account;
	}

	@Override
	public Account addAmount(final long id, final long amount) {
		var account = this.addAmountViaJDBC(id, amount);
		this.cache(account);
		return account;
	}

	protected abstract Account addAmountViaJDBC(final long id, final long amount);

	protected Optional<Account> getViaJedis(final long id) {
		final String key = this.getKey(id);

		final Set<String> members;

		if (this.jedisPool != null) {
			try (final var jedis = this.jedisPool.getResource()) {
				members = jedis.zrevrangeByScore(key, "+inf", "-inf", 0, 1);
			}
		} else {
			members = this.jedisCluster.zrevrangeByScore(key, "+inf", "-inf", 0, 1);
		}

		return members.stream().findFirst().map(this::deserialize);
	}

	protected void cache(final Account account) {
		log.trace("Caching {}@{}: {}", account::id, account::version, account::available);

		if (account.version() < 0) {
			throw new IllegalArgumentException("Invalid version.");
		}

		final var key = this.getKey(account.id());
		final double score = account.version();

		final var member = this.serialize(account);
		final var params = ZAddParams.zAddParams().nx();

		final double minToRemove = 0;
		final double maxToRemove = score - 1;

		final var seconds = EXPIRATION_SECONDS;

		if (this.jedisPool != null) {
			try (
				final var jedis = this.jedisPool.getResource();
				final var pipeline = jedis.pipelined();
			) {
				pipeline.zadd(key, score, member, params);
				pipeline.zremrangeByScore(key, minToRemove, maxToRemove);
				pipeline.expire(key, seconds);
			}
		} else {
			this.jedisCluster.zadd(key, score, member, params);
			this.jedisCluster.zremrangeByScore(key, minToRemove, maxToRemove);
			this.jedisCluster.expire(key, seconds);
		}

		log.trace("Cached {}@{}: {}", account::id, account::version, account::available);
	}

	private String serialize(final Account account) {
		try {
			return this.objectMapper.writeValueAsString(account);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private Account deserialize(final String content) {
		try {
			return this.objectMapper.readValue(content, Account.class);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private String getKey(final long id) {
		return String.format("account:%d", id);
	}

}
