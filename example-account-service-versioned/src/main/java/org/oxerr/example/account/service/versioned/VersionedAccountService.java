package org.oxerr.example.account.service.versioned;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.oxerr.example.account.Account;
import org.oxerr.example.account.service.jdbc.AbstractJDBCAccountService;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.TransientDataAccessResourceException;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class VersionedAccountService extends AbstractJDBCAccountService {

	private static final short DEFAULT_MAX_ATTEMPTS = Short.MAX_VALUE;

	private final Logger log = LogManager.getLogger(VersionedAccountService.class);

	private final String updateSql;

	public VersionedAccountService(
		final DataSource dataSource,
		final JedisPool jedisPool,
		final JedisCluster jedisCluster
	) {
		super(dataSource, jedisPool, jedisCluster);
		this.updateSql = "update account set available = ?, version = version + 1 where id = ? and version = ?";
	}

	@Override
	protected Account addAmountViaJDBC(final long id, final long amount) {
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
					if (rs.next()) {
						available = rs.getLong("available");
						version = rs.getLong("version");
					} else {
						throw new IllegalArgumentException(String.format("No such account with ID %d.", id));
					}
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

		return new Account(id, available, version);
	}

}

