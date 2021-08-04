package org.oxerr.example.account;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MySQLDataSourceFactory {

	private final Logger log = LogManager.getLogger(MySQLDataSourceFactory.class);

	public HikariDataSource getDataSource() {
		final var poolSize = 100;
		final var minIdle = 100;

		var jdbcUrl = "jdbc:mysql://localhost/example";

		var hikariConfig = new HikariConfig();
		hikariConfig.setJdbcUrl(jdbcUrl);
		hikariConfig.setUsername("example");
		hikariConfig.setPassword("G9&zqkNv3*XA8i2#");
		hikariConfig.setMaximumPoolSize(poolSize);
		hikariConfig.setMinimumIdle(minIdle);
		log.trace("hikariConfig: {}", () -> ToStringBuilder.reflectionToString(hikariConfig, ToStringStyle.MULTI_LINE_STYLE));

		return new HikariDataSource(hikariConfig);

	}

}
