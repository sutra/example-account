package org.oxerr.example.account.service.database.lock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.oxerr.example.account.Account;
import org.oxerr.example.account.service.jdbc.AbstractJDBCAccountService;
import org.springframework.dao.TransientDataAccessResourceException;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class DatabaseLockAccountService extends AbstractJDBCAccountService {

	private final String selectForUpdateSql;

	protected DatabaseLockAccountService(
		final DataSource dataSource,
		final JedisPool jedisPool,
		final JedisCluster jedisCluster
	) {
		super(dataSource, jedisPool, jedisCluster);
		this.selectForUpdateSql = "select id, available, version from account where id = ? for update";
	}

	@Override
	protected Account addAmountViaJDBC(final long id, final long amount) {
		try {
			return this.addAmountViaJDBCInternal(id, amount);
		} catch (SQLException e) {
			throw new TransientDataAccessResourceException(e.getMessage(), e);
		}
	}

	private Account addAmountViaJDBCInternal(final long id, final long amount) throws SQLException {
		try (final var conn = this.dataSource.getConnection()) {
			final var originalAutoCommit = conn.getAutoCommit();
			try {
				conn.setAutoCommit(false);
				return this.addAmountViaJDBCInternal(conn, id, amount);
			} finally {
				conn.setAutoCommit(originalAutoCommit);
			}
		}
	}

	private Account addAmountViaJDBCInternal(final Connection conn, final long id, final long amount) throws SQLException {
		if (conn.getAutoCommit()) {
			throw new IllegalArgumentException("Connection should not be in auto-commit mode.");
		}

		long available;
		long version;

		try (final var selectForUpdate = conn.prepareStatement(this.selectForUpdateSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
			selectForUpdate.setLong(1, id);

			try (final var rs = selectForUpdate.executeQuery()) {
				if (rs.next()) {
					available = rs.getLong("available") + amount;
					version = rs.getLong("version") + 1;

					rs.updateLong("available", available);
					rs.updateLong("version", version);

					rs.updateRow();

					conn.commit();
				} else {
					throw new IllegalArgumentException(String.format("No such account with ID %d.", id));
				}
			} catch (SQLException e) {
				conn.rollback();
				throw e;
			}
		}

		return new Account(id, available, version);
	}

}
