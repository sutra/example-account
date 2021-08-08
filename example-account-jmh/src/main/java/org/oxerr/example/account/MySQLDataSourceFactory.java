package org.oxerr.example.account;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * $ mysql -u root -p
 *
 * CREATE DATABASE example;
 * CREATE USER 'example'@'localhost' IDENTIFIED BY 'G9^zqkNv3*XA8i2#';
 * -- ALTER USER 'example'@'localhost' IDENTIFIED BY 'G9^zqkNv3*XA8i2#';
 * GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, DROP, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES ON example.* TO 'example'@'localhost';
 *
 * USE example;
 * CREATE TABLE `account` (
 *   `id` bigint unsigned NOT NULL,
 *   `available` bigint NOT NULL,
 *   `version` bigint unsigned NOT NULL,
 *   PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 *
 * -- SHOW VARIABLES LIKE "max_connections";
 * -- SET GLOBAL max_connections = 500;
 * -- SHOW STATUS WHERE `variable_name` = 'Threads_connected';
 * -- SHOW PROCESSLIST;
 */
public class MySQLDataSourceFactory {

	private final Logger log = LogManager.getLogger(MySQLDataSourceFactory.class);

	public HikariDataSource getDataSource() {
		final var poolSize = 100;
		final var minIdle = 100;

		var jdbcUrl = "jdbc:mysql://localhost/example";

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
