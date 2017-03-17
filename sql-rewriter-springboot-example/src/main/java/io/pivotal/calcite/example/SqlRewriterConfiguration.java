package io.pivotal.calcite.example;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by tzoloc on 3/17/17.
 */
@Configuration
public class SqlRewriterConfiguration {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	@Bean
	@ConfigurationProperties("calcite.datasource")
	public DataSource dataSource(@Autowired String inlineModel) {
		log.info("Calcite Model: " + inlineModel);

		return DataSourceBuilder
				.create()
				.driverClassName("org.apache.calcite.jdbc.Driver")
				.url("jdbc:calcite:lex=JAVA;model=inline:" + inlineModel)
				.build();
	}

	@Bean
	public String inlineModel(
			@Value("${calcite.journalVersionType:TIMESTAMP}") String journalVersionType,
			@Value("${calcite.journalDefaultKey}") String journalDefaultKey,
			@Value("${calcite.journalTables}") String journalTables,
			@Value("${calcite.journalSubsequentVersionField:subsequent_version_number}") String journalSubsequentVersionField,
			@Value("${calcite.journalVersionField:version_number}") String journalVersionField,
			@Value("${calcite.journalSuffix:_journal}") String journalSuffix,
			@Value("${postgres.jdbcUrl}") String postgresJdbcUrl,
			@Value("${postgres.jdbcDriver}") String postgresJdbcDriver,
			@Value("${postgres.jdbcUser}") String postgresJdbcUser,
			@Value("${postgres.jdbcPassword}") String postgresJdbcPassword) {

		String model = "{\n" +
				"  \"version\": \"1.0\",\n" +
				"  \"defaultSchema\": \"doesntmatter\",\n" +
				"  \"schemas\": [\n" +
				"    {\n" +
				"      \"name\": \"hr\",\n" +
				"      \"type\": \"custom\",\n" +
				"      \"factory\": \"org.apache.calcite.adapter.jdbc.JournalledJdbcSchema$Factory\",\n" +
				"      \"operand\": {\n" +
				"        \"jdbcSchema\": \"hr\",\n" +
				"        \"journalSuffix\": \"" + journalSuffix + "\",\n" +
				"        \"journalVersionField\": \"" + journalVersionField + "\",\n" +
				"        \"journalSubsequentVersionField\": \"" + journalSubsequentVersionField + "\",\n" +
				"        \"journalDefaultKey\": \"" + journalDefaultKey + "\",\n" +
				"        \"journalVersionType\": \"" + journalVersionType + "\",\n" +
				"        \"journalTables\": {" + journalTables + "},\n" +
				"        \"jdbcUrl\": \"" + postgresJdbcUrl + "\",\n" +
				"        \"jdbcDriver\": \"" + postgresJdbcDriver + "\",\n" +
				"        \"jdbcUser\": \"" + postgresJdbcUser + "\",\n" +
				"        \"jdbcPassword\": \"" + postgresJdbcPassword + "\"\n" +
				"      }\n" +
				"    }\n" +
				"  ]\n" +
				"}";

		return model;
	}
}
