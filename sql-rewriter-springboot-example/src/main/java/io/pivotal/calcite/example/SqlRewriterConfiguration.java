/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class SqlRewriterConfiguration {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	@Bean
	public JdbcTemplate calciteJdbcTemplate(DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

	@Bean
	@ConfigurationProperties("calcite.datasource")
	public DataSource calciteDataSource(@Autowired String inlineModel) {
		DataSource dataSource = DataSourceBuilder
				.create()
				.driverClassName("org.apache.calcite.jdbc.Driver")
				.url("jdbc:calcite:lex=JAVA;model=inline:" + inlineModel)
				.build();

		log.info("Generated Calcite mode: " + inlineModel);
		return dataSource;
	}

	@Bean
	public JdbcTemplate postgresJdbcTemplate(
			@Value("${postgres.jdbcUrl}") String postgresJdbcUrl,
			@Value("${postgres.jdbcDriver}") String postgresJdbcDriver,
			@Value("${postgres.jdbcUser}") String postgresJdbcUser,
			@Value("${postgres.jdbcPassword}") String postgresJdbcPassword) {

		DataSource targetDataSource = DataSourceBuilder
				.create()
				.driverClassName(postgresJdbcDriver)
				.url(postgresJdbcUrl)
				.username(postgresJdbcUser)
				.password(postgresJdbcPassword)
				.build();

		return new JdbcTemplate(targetDataSource);
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
