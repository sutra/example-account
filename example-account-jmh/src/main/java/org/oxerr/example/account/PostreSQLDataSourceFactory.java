package org.oxerr.example.account;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * $ psql -U postgres
 *
 * CREATE USER example WITH ENCRYPTED PASSWORD 'G9^zqkNv3*XA8i2#';
 * -- ALTER USER example WITH PASSWORD 'G9^zqkNv3*XA8i2#';
 * CREATE DATABASE example OWNER example;
 * \q
 *
 * $ psql -U example
 *
 * CREATE TABLE account (
 * 	id int8 NOT NULL,
 * 	available int8 NOT NULL,
 * 	"version" int8 NOT NULL,
 * 	CONSTRAINT account_pk PRIMARY KEY (id)
 * );
 *
 * -- SHOW max_connections;
 */
public class PostreSQLDataSourceFactory {

	private final Logger log = LogManager.getLogger(PostreSQLDataSourceFactory.class);

	public HikariDataSource getDataSource() {
		final var poolSize = 100;
		final var minIdle = 100;

		var jdbcUrl = "jdbc:postgresql://localhost/example";

		var hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(jdbcUrl);
		hikariConfig.setUsername("example");
		hikariConfig.setPassword("G9^zqkNv3*XA8i2#");
		hikariConfig.setMaximumPoolSize(poolSize);
		hikariConfig.setMinimumIdle(minIdle);
		log.trace("hikariConfig: {}", () -> ToStringBuilder.reflectionToString(hikariConfig, ToStringStyle.MULTI_LINE_STYLE));

		return new HikariDataSource(hikariConfig);
	}

}
