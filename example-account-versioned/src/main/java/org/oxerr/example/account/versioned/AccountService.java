package org.oxerr.example.account.versioned;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.oxerr.example.account.Account;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.TransientDataAccessResourceException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ZAddParams;

public class AccountService {

	private static final short DEFAULT_MAX_ATTEMPTS = Short.MAX_VALUE;

	private static final long EXPIRATION_SECONDS = Duration.ofDays(1).toMinutes() * 60;

	private final Logger log = LogManager.getLogger(AccountService.class);

	private final DataSource dataSource;

	private final JedisPool jedisPool;

	private final ObjectMapper objectMapper;

	private final String selectSql;

	private final String updateSql;

	public AccountService(DataSource dataSource, JedisPool jedisPool) {
		this.dataSource = dataSource;
		this.jedisPool = jedisPool;

		this.objectMapper = new ObjectMapper();

		this.selectSql = "select id, available, version from account where id = ?";
		this.updateSql = "update account set available = ?, version = version + 1 where id = ? and version = ?";
	}

	public Account newAccount(final long id) {
		final var insertSql = "insert into account(id, available, version) values(?, ?, ?)";
		try (
			final var conn = this.dataSource.getConnection();
			final var insert = conn.prepareStatement(insertSql);
		) {
			insert.setLong(1, id);
			insert.setLong(2, 0);
			insert.setLong(3, 0);
			insert.executeUpdate();
		} catch (SQLException e) {
			log.warn(e.getMessage());
		}
		return this.get(id);
	}

	public Account addAmount(final long id, final long amount) {
		final var maxAttempts = DEFAULT_MAX_ATTEMPTS;

		var available = 0L;
		var version = -1L;

		var attempts = 0;

		try (
			final var conn = this.dataSource.getConnection();
			final var select = conn.prepareStatement(this.selectSql);
			final var update = conn.prepareStatement(this.updateSql);
		) {
			select.setLong(1, id);

			while (true) {

				// Read available and version from database.
				try (final ResultSet rs = select.executeQuery()) {
					while (rs.next()) {
						available = rs.getLong("available");
						version = rs.getLong("version");
					}
				}

				if (version == -1) {
					throw new IllegalArgumentException(String.format("No such account with ID %d.", id));
				}


				// Update the available and version.
				update.setLong(1, available + amount);
				update.setLong(2, id);
				update.setLong(3, version);

				final int count = update.executeUpdate();
				log.trace("updated {}@{}, row count: {}, attempts: {}.", id, version + 1, count, attempts);

				if (count >= 1) {

					// The version should be incremented after updated.
					available = available + amount;
					version = version + 1;

					break;
				} else {
					if (++attempts >= maxAttempts) {
						log.warn("Update available failed. {}@{}, attempts: {}.", id, version + 1, attempts);
						throw new OptimisticLockingFailureException("Update available failed.");
					}
				}
			}
		} catch (SQLException e) {
			throw new TransientDataAccessResourceException(e.getMessage(), e);
		}

		this.cache(id, version, available);

		return new Account(id, available, version);
	}

	protected void cache(final long id, final long version, final long available) {
		log.trace("Caching {}@{}: {}", id, version, available);

		if (version < 0) {
			throw new IllegalArgumentException("Invalid version.");
		}

		final var key = this.getKey(id);
		final double score = version;

		final var account = new Account(id, available, version);
		final var member = this.serialize(account);
		final var params = ZAddParams.zAddParams().nx();

		final double minToRemove = 0;
		final double maxToRemove = score - 1;

		final var seconds = EXPIRATION_SECONDS;

		try (
			final var jedis = this.jedisPool.getResource();
			final var pipeline = jedis.pipelined();
		) {
			pipeline.zadd(key, score, member, params);
			pipeline.zremrangeByScore(key, minToRemove, maxToRemove);
			pipeline.expire(key, seconds);
		}

		log.trace("Cached {}@{}: {}", id, version, available);
	}

	public Account get(final long id) {
		log.trace("Getting {}", id);

		final String key = this.getKey(id);

		final Set<String> members;

		try (final var jedis = this.jedisPool.getResource()) {
			members = jedis.zrevrangeByScore(key, "+inf", "-inf", 0, 1);
		}

		return members.stream().findFirst().map(this::deserialize).orElseGet(() -> {
			final var account = this.getViaJdbc(id);
			cache(account.id(), account.version(), account.available());
			return account;
		});
	}

	protected Account getViaJdbc(final long id) {
		log.trace("Getting via JDBC: {}", id);

		try {
			return this.getViaJdbcInternal(id);
		} catch (SQLException e) {
			throw new TransientDataAccessResourceException(e.getMessage(), e);
		}
	}

	private Account getViaJdbcInternal(final long id) throws SQLException {
		long available = 0;
		long version = -1;

		try (
			final var conn = this.dataSource.getConnection();
			final var select = conn.prepareStatement(this.selectSql);
		) {
			select.setLong(1, id);

			try (final ResultSet rs = select.executeQuery()) {
				while (rs.next()) {
					available = rs.getLong("available");
					version = rs.getLong("version");
				}
			}
		}

		if (version == -1) {
			throw new IllegalArgumentException(String.format("No such account with ID %d.", id));
		}

		return new Account(id, available, version);
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

