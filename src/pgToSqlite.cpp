/*
   pgToSqlite  C++ tool to dump a PostgreSQL database to SQLite3.
    Copyright (C) 2013-2020  Oliver Freyermuth
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <Options.h>

#include <iostream>
#include <iomanip>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string>
#include <string.h>
#include <sstream>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>

#include <arpa/inet.h>

#include <netdb.h>

#include <set>
#include <vector>

#include <climits>

#include <sqlite3.h>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>

std::string getHostFromName(const char *host) {
	struct addrinfo hints, *res;
	int errcode;
	char addrstr[100];
	void *ptr = nullptr;

	memset (&hints, 0, sizeof (hints));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags |= AI_CANONNAME;

	errcode = getaddrinfo (host, nullptr, &hints, &res);
	if (errcode != 0)	{
		perror ("getaddrinfo");
		std::string ipAddr("localhost");
		return ipAddr;
	}

	//printf ("Host: %s\n", host);
	while (res) {
		inet_ntop (res->ai_family, res->ai_addr->sa_data, addrstr, 100);

		switch (res->ai_family) {
			case AF_INET:
				ptr = &((struct sockaddr_in *) res->ai_addr)->sin_addr;
				break;
			case AF_INET6:
				ptr = &((struct sockaddr_in6 *) res->ai_addr)->sin6_addr;
				break;
		}
		if (ptr == nullptr) {
			std::cerr << "Error during hostname-resolution, defaulting to localhost!" << std::endl;
			freeaddrinfo(res);
			std::string ipAddr(addrstr);
			return ipAddr;
		}
		inet_ntop (res->ai_family, ptr, addrstr, 100);
		std::cout << "Hostname lookup for " << host << " returned: " << std::endl;
		std::cout << "IPv" << (res->ai_family == PF_INET6 ? 6 : 4) << " address: "
		          << addrstr << " (" << res->ai_canonname << ")" << std::endl;
		std::cout << "Taking first result from hostname-lookup!" << std::endl;
		freeaddrinfo(res);
		std::string ipAddr(addrstr);
		return ipAddr;
		//res = res->ai_next;
	}
	std::string ipAddr("localhost");
	return ipAddr;
}

void beginSQLiteTransaction(sqlite3 *sqliteDB) {
	char *sqlErrorMsg;
	sqlite3_exec(sqliteDB, "BEGIN TRANSACTION;", nullptr, nullptr, &sqlErrorMsg);
	if (sqlErrorMsg != nullptr) {
		std::cerr << std::setw(10) << "" << "Error starting SQLite3-transaction!" << std::endl;
		std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
		std::cerr << std::setw(10) << "" << "Continuing without..." << std::endl;
	}
	sqlite3_free(sqlErrorMsg);
}
void endSQLiteTransaction(sqlite3 *sqliteDB) {
	char *sqlErrorMsg;
	sqlite3_exec(sqliteDB, "END TRANSACTION;", nullptr, nullptr, &sqlErrorMsg);
	if (sqlErrorMsg != nullptr) {
		std::cerr << std::setw(10) << "" << "Error ending SQLite3-transaction!" << std::endl;
		std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
		std::cerr << std::setw(10) << "" << "Trying to continue..." << std::endl;
	}
	sqlite3_free(sqlErrorMsg);
}
void beginPGSQLTransaction(PGconn *dbc) {
	PGresult* res = PQexec(dbc, "BEGIN");
	if (!(PQresultStatus(res) == PGRES_COMMAND_OK)) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		exit(1);
	}
	PQclear(res);
}
void endPGSQLTransaction(PGconn *dbc) {
	PGresult* res = PQexec(dbc, "COMMIT");
	if (!(PQresultStatus(res) == PGRES_COMMAND_OK)) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		exit(1);
	}
	PQclear(res);
}

void dropLOsizeFun(PGconn *dbc) {
	PGresult* res = PQexec(dbc, "DROP FUNCTION IF EXISTS get_lo_size(oid);");
	if (!(PQresultStatus(res) == PGRES_COMMAND_OK)) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		exit(1);
	}
	PQclear(res);
}

size_t getLargeObjectSize(PGconn *dbc, int oid) {
	char query[1024];
	sprintf(query, "select sum(length(lo.data)) from pg_largeobject lo where lo.loid=%d;", oid);
	PGresult* res = PQexec(dbc, query);
	if (!((PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_COMMAND_OK))) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		return -1;
	}

	if (PQresultStatus(res) == PGRES_TUPLES_OK ) {
		size_t lObjSize = atoi(PQgetvalue(res, 0, 0));

		PQclear(res);
		return lObjSize;
	}
	return 0;
}

size_t getLargeObjectSize_v2(PGconn *dbc, int oid) {
	static bool haveFunction = false;

	std::string sql_query = "";
	std::stringstream buildquery;

	if (!haveFunction) {
		buildquery << "DROP FUNCTION IF EXISTS get_lo_size(oid); CREATE OR REPLACE FUNCTION get_lo_size(oid) RETURNS bigint AS $$ "
		           << "DECLARE \n"
		           << "    fd integer; \n"
		           << "    sz bigint; \n"
		           << "BEGIN \n"
		           << "    -- Open the LO; N.B. it needs to be in a transaction otherwise it will close immediately. \n"
		           << "    -- Luckily a function invocation makes its own transaction if necessary. \n"
		           << "    -- The mode x'40000'::int corresponds to the PostgreSQL LO mode INV_READ = 0x40000. \n"
		           << "    fd := lo_open($1, x'40000'::int); \n"
		           << "    -- Seek to the end.  2 = SEEK_END. \n"
		           << "    PERFORM lo_lseek(fd, 0, 2); \n"
		           << "    -- Fetch the current file position; since we're at the end, this is the size. \n"
		           << "    sz := lo_tell(fd); \n"
		           << "    -- Remember to close it, since the function may be called as part of a larger transaction. \n"
		           << "    PERFORM lo_close(fd); \n"
		           << "    -- Return the size. \n"
		           << "    RETURN sz; \n"
		           << "END; $$ LANGUAGE 'plpgsql' VOLATILE STRICT; ";

		sql_query = buildquery.str();
		PGresult* res = PQexec(dbc, sql_query.c_str());
		if (!((PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_COMMAND_OK))) {
			std::cerr << PQerrorMessage(dbc) << std::endl;
			return -1;
		} else {
			PQclear(res);
			haveFunction = true;
		}
	}

	buildquery.str("");
	buildquery.clear();
	buildquery << "SELECT get_lo_size(" << oid << ");";
	sql_query = buildquery.str();

	PGresult* res = PQexec(dbc, sql_query.c_str());
	if (!((PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_COMMAND_OK))) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		return -1;
	} else {
		//std::cout << "Got size: " << atoi(PQgetvalue(res,0,0)) << std::endl;
		size_t lObjSize = atol(PQgetvalue(res, 0, 0));
		PQclear(res);
		return lObjSize;
	}
	return 0;
}

int main(int argc, char *argv[]) {
	options::parser parser("PostgreSQL to SQLite dumper. Connects to a PostgreSQL database, enumerates all tables and their columns, and generates analogous structure in an SQLite database. Large objects are supported and converted to blobs.");

	options::single<std::string> dbHost('H', "dbHost", "PostgreSQL database host", "localhost");
	options::single<unsigned>    dbPort('p', "dbPort", "PostgreSQL database port (if not default)", 5432);
	options::single<std::string> dbName('d', "dbName", "PostgreSQL database name");
	options::single<std::string> dbUser('U', "dbUser", "PostgreSQL database user");
	options::single<std::string> dbPassword('P', "dbPassword", "PostgreSQL database user's password");
	options::single<std::string> sqliteFilename('f', "sqliteFilename", "Filename for creaed SQLite3-DB, must not exist yet!");
	options::single<std::string> pgTimezone('T', "dbTimeZone", "Local time zone of the PostgreSQL server, needed to convert 'timestamp without time zone' columns.", "Europe/Berlin");
	options::container<std::string> excludeTables('x', "excludeTable", "Exclude this table from dump. Interpreted with 'NOT LIKE' so SQL-patterns are allowed.");

	options::single<bool> dumpLargeObjects('Q', "dumpLargeObjects", "Dump large objects.", true);
	options::single<bool> useMaxDumpSize('B', "useMaxDumpSize", "Exclude tables larger 1 GiB from dump.", true);
	options::single<bool> useSelectOnly('O', "useSelectOnly", "Use 'SELECT ONLY' statements and include child tables. Otherwise, childs are excluded and accounted to their parent's size ('SELECT' includes their rows).", false);

	parser.fRequire({&dbName, &sqliteFilename});

	auto unusedOptions = parser.fParse(argc, argv);

	int opt = 0;

	if (!excludeTables.empty()) {
		std::cout << "Will exclude the following tables / table patterns from dump:" << std::endl;
		for (const auto & excludeTable : excludeTables) {
			std::cout << " - " << excludeTable.c_str() << std::endl;
		}
	}

	std::string connectStr;
	{
		std::stringstream buildConnectStr;
		buildConnectStr << "hostaddr='" << getHostFromName(dbHost.c_str()) << "' "
		                << "port='"     << dbPort                          << "' "
		                << "dbname='"   << dbName                          << "' "
		                << "user='"     << dbUser                          << "' "
		                << "password='" << dbPassword                      << "' "
		                << "connect_timeout='10'";
		connectStr = buildConnectStr.str();
	}

	std::cout << "Connecting to Postgres, using: \"" << connectStr << "\"... " << std::endl;
	PGconn* dbc = PQconnectdb(connectStr.c_str());

	// Set timezone to UTC because we want to store timestamps in UTC in SQLite, too:
	PGresult* res = PQexec(dbc, "SET TIMEZONE TO 'UTC';");
	if (!(PQresultStatus(res) == PGRES_COMMAND_OK)) {
		std::cerr << PQerrorMessage(dbc) << std::endl;
		return -1;
	}

	// Postgres is open, then we can now open sqlite
	// Database-variables:
	sqlite3 *sqliteDB;
	int sql_ret;

	{
		struct stat buffer;
		if (stat(sqliteFilename.c_str(), &buffer) == 0) {
			std::cerr << "File " << sqliteFilename << " already exists! Will not delete it and stop here." << std::endl;
			return (-1);
		}
	}
	// Create sqlite-DB:
	sql_ret = sqlite3_open(sqliteFilename.c_str(), &sqliteDB);
	if (sql_ret) {
		std::cerr << "FATAL: Can't open database: " << sqliteFilename << " Error: " << sqlite3_errmsg(sqliteDB) << std::endl;
		sqlite3_close(sqliteDB);
		exit(1);
	}

	// Before the big insertion begins, disable autocommit, or it will break your disk ;-)
	beginSQLiteTransaction(sqliteDB);

	// Now, request table-names from postgres:
	{
		std::stringstream buildquery;
		buildquery << "SELECT "
		           <<  "   table_name,  "
		           <<  "   (CASE WHEN table_name::regclass IN (SELECT inhrelid FROM pg_inherits) THEN 1 ELSE 0 END) AS is_child "
		           <<  " FROM "
		           <<  "   information_schema.tables "
		           <<  " WHERE";
		buildquery <<  "   table_schema NOT IN ('pg_catalog', 'information_schema') ";
		for (const auto & excludeTable : excludeTables) {
			buildquery << " AND table_name NOT LIKE $dollarQuote$" << excludeTable << "$dollarQuote$ " << std::endl;
		}
		buildquery << " ; " << std::endl;
		res = PQexec(dbc, buildquery.str().c_str());
		if (!((PQresultStatus(res) == PGRES_TUPLES_OK) || (PQresultStatus(res) == PGRES_COMMAND_OK))) {
			std::cerr << "Query failed: " << std::endl;
			std::cerr << buildquery.str() << std::endl;
			std::cerr << PQerrorMessage(dbc) << std::endl;
			return -1;
		}
	}

	if ((PQresultStatus(res) == PGRES_TUPLES_OK) && (PQnfields(res) == 2)) {
		for (int tb = 0; tb < PQntuples(res); tb++) { // These are the table-name-rows

			// Columns that contain large objects:
			std::set<int> largeObjectColumns;

			// Columns that contain timestamps with timezone:
			std::set<int> timeZoneColumns;

			// Columns that contain timestamps which might be infinite:
			std::set<int> timeStampColumns;

			// Column-names used when selecting the columns from postgres.
			// This can also contain conversions, e.g. for timestamps without time zone.
			std::vector<std::string> colNamesForPqSelect;

			// Triggers to be created after table-creation.
			std::vector<std::string> sqliteTriggers;

			std::stringstream sqlite_create_query;
			std::stringstream sqlite_insert_query;

			// Get table name here:
			std::string tableName = PQgetvalue(res, tb, 0);
			//std::cout << PQfname(res,j) << ": " << tableName << std::endl;

			// Is it a child-table?
			bool isChildTable = (atoi(PQgetvalue(res, tb, 1)) == 1) ? true : false;

			if (isChildTable) {
				if (!useSelectOnly) {
					// Then we do not want child tables!
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " "
					          << "Is child-table, not in SELECT ONLY mode, skipping!" << std::endl;
					continue;
				} else {
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " "
					          << "Is a child-table!" << std::endl;
				}
			}

			/*
			  if (excludeTables.find(tableName) != excludeTables.end()) {
			  // We do not want this table!
			  std::cout << "[" << tableName << "]"
			  << std::setw(32 - tableName.length()) << " "
			  << "Table in exclude list, skipping!" << std::endl;
			  continue;
			  }
			*/

			sqlite_create_query << "CREATE TABLE " << tableName << " (";
			sqlite_insert_query << "INSERT INTO " << tableName << " VALUES (";

			std::string sql_query = "";
			std::stringstream buildquery;

			// Select column names and datatypes:
			{
				buildquery.str("");
				buildquery.clear();
				buildquery <<
				           "select "
				           "   column_name, "
				           "   column_default, "
				           "   data_type    "
				           " from "
				           "   information_schema.columns "
				           " where"
				           "   table_name='" << tableName << "'"
				           " order by"
				           "   ordinal_position;";
				sql_query = buildquery.str();
				PGresult* res2 = PQexec(dbc, sql_query.data());
				if (!((PQresultStatus(res2) == PGRES_TUPLES_OK) || (PQresultStatus(res2) == PGRES_COMMAND_OK))) {
					std::cerr << PQerrorMessage(dbc) << std::endl;
					return -1;
				}

				if (PQresultStatus(res2) == PGRES_TUPLES_OK ) {
					int rowCount = PQntuples(res2);
					int colCount = PQnfields(res2);
					for (int row = 0; row < rowCount; row++) { // These result-rows are the columns of the table!
						if (colCount != 3) {
							std::cerr << "More than two columns in (name,default,type) query, something very wrong!!!" << std::endl;
							exit(1);
						}

						// Echo column name here:
						std::string colName = PQgetvalue(res2, row, 0);
						sqlite_create_query << colName << " ";

						// Echo column default here:
						std::string colDefault = PQgetvalue(res2, row, 1);

						// Echo column type here:
						std::string colType = PQgetvalue(res2, row, 2);
						if (colType.find("-") != std::string::npos) {
							// PostgreSQL allows for strange characters in column types.
							// Up to now, only "-" is known (as in USER-DEFINED).
							// We just replace that with a space...
							std::replace(colType.begin(), colType.end(), '-', ' ');
						}

						{
							if ((colDefault.find("nextval(") != std::string::npos) && (colDefault.find("seq'::regclass)") != std::string::npos)) {
								if (colType == "integer") {
									// Looks like an autoincrement... create matching trigger!
									std::string triggerQuery = "CREATE TRIGGER " + tableName + "_" + colName + "_autoincrement AFTER INSERT ON " + tableName + "";
									triggerQuery += " FOR EACH ROW when new." + colName + " is NULL ";
									triggerQuery += " BEGIN ";
									triggerQuery += " UPDATE " + tableName + " SET " + colName + " = (SELECT IFNULL(MAX(" + colName + ")+1,0) FROM " + tableName + ") WHERE rowid = new.rowid;";
									triggerQuery += " END; ";

									//std::cout << triggerQuery << std::endl;
									sqliteTriggers.push_back(triggerQuery);
								}
								// Dirty hack: No default value then.
								colDefault = "";
							} else {
								// Maybe this is a nice default we can also use?
								if (colDefault == "now()") {
									colDefault = "CURRENT_TIMESTAMP";
								} else if (colDefault.find("'infinity'::timestamp") == 0) {
									colDefault = "'9999-12-31 12:00:00'";
								} else if (colDefault.find("'-infinity'::timestamp") == 0) {
									colDefault = "'0000-00-00 12:00:00'";
								} else if (colDefault.find("'Infinity'") != std::string::npos) {
									colDefault = "9e999";
								} else if (colDefault.find("'-Infinity'") != std::string::npos) {
									colDefault = "-9e999";
								} else if (colDefault.find("::") != std::string::npos) {
									// Im feelin' lucky!
									colDefault.erase(colDefault.find("::"), std::string::npos);
								}
							}
						}

						sqlite_create_query << colType;

						if (colDefault.length() > 0) {
							sqlite_create_query << " default " << colDefault;
						}

						if (colType == "oid") {
							// Blobby stuff encountered!
							largeObjectColumns.insert(row);
						}

						if (colType.find("with time zone") != std::string::npos) {
							// Column with time zone encountered, need to take special care (cut off the +00!)
							timeZoneColumns.insert(row);
						}

						if (colType.find("timestamp") != std::string::npos) {
							// Column with time stamp encountered, need to take special care for infinity stuff
							timeStampColumns.insert(row);
						}

						if (colType.find("without time zone") != std::string::npos) {
							// Column without time zone encountered, need to take special care.
							// Postgres stores and displays these IN LOCAL TIME of the database server.
							// We don't want this SQLite prefers UTC for string-matching.
							colName += " at time zone '" + pgTimezone.fGetValue() + "'";

							// Also for columns without time zone we will get '+00'
							// when doing the typecast-select.
							timeZoneColumns.insert(row);
						}

						colNamesForPqSelect.push_back(colName);

						sqlite_insert_query << "?";// << i;
						if (row != rowCount - 1) {
							sqlite_create_query << ", ";
							sqlite_insert_query << ", ";
						}
					}
				}
				PQclear(res2);
			}

			{
				// now, we can create the corresponding table in SQLite:
				sqlite_create_query << ");";
				sql_query = sqlite_create_query.str();
				//std::cout << sql_query << std::endl;

				char *sqlErrorMsg;
				sqlite3_exec(sqliteDB, sql_query.c_str(), nullptr, nullptr, &sqlErrorMsg);
				if (sqlErrorMsg != nullptr) {
					std::cerr << std::setw(10) << "" << "Error creating table '" << tableName << "'!" << std::endl;
					std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
					std::cerr << std::setw(10) << "" << "Query: " << sql_query << std::endl;
					std::cerr << std::setw(10) << "" << "Ignoring..." << std::endl;
				}
				sqlite3_free(sqlErrorMsg);
			}

			{
				// now, we can create the needed triggers in SQLite:
				//std::cout << sql_query << std::endl;
				if (sqliteTriggers.size() > 0) {
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " "
					          << std::setw(7) << sqliteTriggers.size() << " autoincrements, recreating...";

					for (auto & sqlQuery : sqliteTriggers) {

						char *sqlErrorMsg;
						sqlite3_exec(sqliteDB, sqlQuery.c_str(), nullptr, nullptr, &sqlErrorMsg);
						if (sqlErrorMsg != nullptr) {
							std::cerr << std::setw(10) << "" << "Error creating trigger!" << std::endl;
							std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
							std::cerr << std::setw(10) << "" << "Query: " << sqlQuery << std::endl;
							std::cerr << std::setw(10) << "" << "Ignoring..." << std::endl;
						}
						sqlite3_free(sqlErrorMsg);
						std::cout << ".";
						fflush(stdout);
					}
					std::cout << "done!" << std::endl;
					sqliteTriggers.clear();
				}
			}

			sqlite_insert_query << ");";
			sql_query = sqlite_insert_query.str();
			//std::cout << sql_query << std::endl;
			sqlite3_stmt *insertStmt;
			const char *lastReadChar;
			int ret = sqlite3_prepare_v2(sqliteDB, sql_query.c_str(), -1, &insertStmt, &lastReadChar);
			if (ret != SQLITE_OK) {
				std::cerr << std::setw(10) << "" << "Error preparing insert-query, error " << ret << " " << sqlite3_errmsg(sqliteDB) << "!" << std::endl;
				std::cerr << std::setw(10) << "" << "Query was: " << std::endl;
				std::cerr << std::setw(10) << "" << sql_query << std::endl;
				std::cerr << std::setw(10) << "" << "As this might be a caused by something fancy" << std::endl;
				std::cerr << std::setw(10) << "" << "you may not need, we just skip it!" << std::endl;
				continue;
			}

			// Now, we can build the select-query for postgres
			{
				// We do each table in one transaction for PGSQL:
				beginPGSQLTransaction(dbc);

				// Check how large the table is, so the user can see what he/she is up to!

				buildquery.str("");
				buildquery.clear();
				if (!useSelectOnly) {
					// Have to include sizes of child-tables in calculation!
					buildquery << "SELECT "
					           << " pg_size_pretty(pg_total_relation_size('" << tableName << "')), "
					           << " pg_total_relation_size('" << tableName << "') "
					           << " ;";
					// Based on: http://dba.stackexchange.com/a/63935
					buildquery << "SELECT "
					           << " pg_size_pretty(COALESCE(sum(pg_total_relation_size(i.inhrelid::regclass))::bigint, 0) + pg_total_relation_size('" << tableName << "')), "
					           << " COALESCE(sum(pg_total_relation_size(i.inhrelid::regclass))::bigint, 0) + pg_total_relation_size('" << tableName << "') "
					           << " FROM   pg_inherits i "
					           << " WHERE  i.inhparent = '" << tableName << "'::regclass"
					           << " ;";
				} else {
					buildquery << "SELECT "
					           << " pg_size_pretty(pg_total_relation_size('" << tableName << "')), "
					           << " pg_total_relation_size('" << tableName << "') "
					           << " ;";
				}
				sql_query = buildquery.str();
				PGresult* resBytes = PQexec(dbc, sql_query.data());
				if (!((PQresultStatus(resBytes) == PGRES_TUPLES_OK) || (PQresultStatus(resBytes) == PGRES_COMMAND_OK))) {
					std::cerr << PQerrorMessage(dbc) << std::endl;
					return -1;
				}

				const char* tableSizePretty = strdup(PQgetvalue(resBytes, 0, 0));
				long long tableSizeBytes  = std::atoll(PQgetvalue(resBytes, 0, 1));
				PQclear(resBytes);

				if (useMaxDumpSize == true) {
					long long maxDumpSize = 1;
					maxDumpSize *= 1024;
					maxDumpSize *= 1024;
					maxDumpSize *= 1024;

					if (tableSizeBytes > maxDumpSize) {
						std::cerr << "[" << tableName                << "]" << " Table size is " << tableSizeBytes << " bytes (= " << tableSizePretty << ")!!!" << std::endl;
						std::cerr << std::setw(tableName.length() + 2) << ""  << " This size exceeds 1 GiB," << std::endl;
						std::cerr << std::setw(tableName.length() + 2) << ""  << " refusing to dump this, skipping table!" << std::endl;
						std::cerr << std::setw(tableName.length() + 2) << ""  << " You can override this behaviour with the -B parameter." << std::endl;
						endPGSQLTransaction(dbc);
						continue;
					}
				}

				bool tableNamePrinted = false;

				// Query columns which have indexes.
				// See also: http://stackoverflow.com/questions/2204058/show-which-columns-an-index-is-on-in-postgresql
				buildquery.str("");
				buildquery.clear();
				buildquery << " select "
				           << "  i.relname as index_name,"
				           << "  t.relname as table_name,"
				           << "  array_to_string(array_agg(a.attname), ', ') as column_names"
				           << " from"
				           << "  pg_class t,"
				           << "  pg_class i,"
				           << "  pg_index ix,"
				           << "  pg_attribute a"
				           << " where"
				           << "  t.oid = ix.indrelid"
				           << "  and i.oid = ix.indexrelid"
				           << "  and a.attrelid = t.oid"
				           << "  and a.attnum = ANY(ix.indkey)"
				           << "  and t.relkind = 'r'"
				           << "  and t.relname = '" << tableName << "'"
				           << " group by "
				           << "  t.relname,"
				           << "  i.relname"
				           << " order by"
				           << "  t.relname,"
				           << "  i.relname;";
				sql_query = buildquery.str();
				PGresult* resIndexes = PQexec(dbc, sql_query.data());
				if (!((PQresultStatus(resIndexes) == PGRES_TUPLES_OK) || (PQresultStatus(resIndexes) == PGRES_COMMAND_OK))) {
					std::cerr << PQerrorMessage(dbc) << std::endl;
					return -1;
				}
				int indexesCount = PQntuples(resIndexes);
				if (indexesCount > 0) {
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " "
					          << std::setw(7) << indexesCount << " indexes, recreating...";
					for (int i = 0; i < indexesCount; i++) {
						buildquery.str("");
						buildquery.clear();
						buildquery << "CREATE INDEX "
						           << " '" << PQgetvalue(resIndexes, i, 0) << "'"
						           << "  ON "
						           << " '" << PQgetvalue(resIndexes, i, 1) << "'"
						           << " (" << PQgetvalue(resIndexes, i, 2) << ");";
						sql_query = buildquery.str();
						//std::cout << sql_query << std::endl;
						char *sqlErrorMsg;
						sqlite3_exec(sqliteDB, sql_query.c_str(), nullptr, nullptr, &sqlErrorMsg);
						//std::cout << sqlErrorMsg << std::endl;
						if (sqlErrorMsg != nullptr) {
							std::cerr << std::setw(10) << "" << "Error creating table '" << tableName << "'!" << std::endl;
							std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
							std::cerr << std::setw(10) << "" << "Query: " << sql_query << std::endl;
							std::cerr << std::setw(10) << "" << "Ignoring..." << std::endl;
						}
						sqlite3_free(sqlErrorMsg);
						std::cout << ".";
						fflush(stdout);
					}
					std::cout << "done!" << std::endl;
					//tableNamePrinted = true;
				}
				PQclear(resIndexes);

				buildquery.str("");
				buildquery.clear();
				buildquery << "SELECT ";
				for (auto it = colNamesForPqSelect.begin(); it != colNamesForPqSelect.end(); ++it) {
					buildquery << *it;
					if ((it + 1) != colNamesForPqSelect.end()) {
						buildquery << ",";
					}
				}
				buildquery <<	" FROM ";
				if (useSelectOnly) {
					buildquery << " ONLY ";
				}
				buildquery << "   " << tableName << ";";
				sql_query = buildquery.str();

				if (!tableNamePrinted) {
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " ";
				} else {
					std::cout << std::setw(34) << " ";
				}
				std::cout << "Fetching " << (useSelectOnly ? "ONLY" : "FULL") << " table, size: " << std::setw(10) << tableSizePretty << "..." ;
				printf("\r");
				fflush(stdout);

				PGresult* res3 = PQexec(dbc, sql_query.data());
				if (!((PQresultStatus(res3) == PGRES_TUPLES_OK) || (PQresultStatus(res3) == PGRES_COMMAND_OK))) {
					std::cerr << PQerrorMessage(dbc) << std::endl;
					return -1;
				}
				int rowCount = PQntuples(res3);
				int colCount = PQnfields(res3);
				if (!tableNamePrinted) {
					std::cout << "[" << tableName << "]"
					          << std::setw(32 - tableName.length()) << " ";
				} else {
					std::cout << std::setw(34) << " ";
				}
				std::cout <<             std::setw(10) << tableSizePretty
				          << " from " << std::setw( 7) << rowCount << " rows"
				          << " in "   << std::setw( 3) << colCount << " columns";

				if (dumpLargeObjects != true) {
					largeObjectColumns.clear();
				}

				if (!largeObjectColumns.empty()) {
					std::cout << "." << std::endl;
					std::cout << std::setw(32) << "" << "Table has large objects," << std::endl;
					std::cout << std::setw(32) << "" << "consider fetching a coffee or two!" << std::endl;
				} else {
					std::cout << "." << std::endl;
				}

				fflush(stdout);

				// Before the big insertion begins, disable autocommit, or it will break your disk ;-)
				//beginSQLiteTransaction(sqliteDB);

				for (int i = 0; i < rowCount; i++) {
					for (int j = 0; j < colCount; j++) {
						int ret2 = 0;

						// Is this a large object column?
						if (largeObjectColumns.count(j) != 0) {
							int oid = atoi(PQgetvalue(res3, i, j));
							//std::cout << "found column " << j <<
							std::cout << "  => Retrieving large object oid " << oid << " ";
							size_t lObjSize = getLargeObjectSize_v2(dbc, oid);
							if (lObjSize == 0) {
								std::cerr << "ERROR determining size!";
								dropLOsizeFun(dbc);
								exit(1);
							} else {
								//std::cout << "(size: " << (int)(lObjSize/1024.) << "kB) ";
								std::cout << "(size: " << (lObjSize) << "B) ";
							}

							int lObjFD = lo_open(dbc, oid, INV_READ);

							auto buf = new char[lObjSize];

							size_t readBytes = lo_read(dbc, lObjFD, buf, lObjSize);
							if (readBytes != lObjSize) {
								std::cerr << "Expected " << lObjSize << " bytes, got " << readBytes << "!" << std::endl;
								std::cerr << PQerrorMessage(dbc) << std::endl;
							}

							if (lo_close(dbc, lObjFD) != 0) {
								std::cerr << "Error closing file descriptor to large object with ID " << oid << "!" << std::endl;
								std::cerr << PQerrorMessage(dbc) << std::endl;
								endPGSQLTransaction(dbc);
								dropLOsizeFun(dbc);
								delete [] buf;
								exit(1);
							}

							std::cout << " (row: " << i << "/" << rowCount << ")";
							fflush(stdout);
							printf("\r%80s\r", " ");
							//printf("\r");
							ret2 = sqlite3_bind_blob(insertStmt, j + 1, buf, lObjSize, SQLITE_TRANSIENT);
							delete [] buf;

						} else {
							bool handledSpecially = false;

							bool fieldIsNull = (PQgetisnull(res3, i, j) == 1 ? true : false);

							const char* plainValue = PQgetvalue(res3, i, j);

							// Is this a column with a timestamp with time zone?
							if (timeZoneColumns.count(j) != 0) {
								const char *tsWithZone = plainValue;
								const char *zonePart = strrchr(tsWithZone, '+');
								if (zonePart == nullptr) {
									//std::cerr << "PostgreSQL behaving strangely: Not returning part with time zone for 'with time zone' column!" << std::endl;
									//std::cerr << "Got value: " << "'" << tsWithZone << "', taking as-is." << std::endl;
									//ret2 = sqlite3_bind_text(insertStmt, j+1, PQgetvalue(res3, i, j), -1, SQLITE_STATIC);
								} else {
									char *tsWithoutZone = strdup(tsWithZone);
									tsWithoutZone[zonePart - tsWithZone] = '\0';
									ret2 = sqlite3_bind_text(insertStmt, j + 1, tsWithoutZone, -1, SQLITE_STATIC);
									handledSpecially = true;
								}
							}

							// Is this a timestamp-column that might be infinite, and has not yet been handled?
							if ((!handledSpecially) && (timeStampColumns.count(j) != 0)) {
								if (strcmp(plainValue, "infinity") == 0) {
									// This strange value is our +infty date
									ret2 = sqlite3_bind_text(insertStmt, j + 1, "9999-12-31 12:00:00", -1, SQLITE_STATIC);
									handledSpecially = true;
								} else if (strcmp(plainValue, "-infinity") == 0) {
									// This strange value is our -infty date
									ret2 = sqlite3_bind_text(insertStmt, j + 1, "0000-00-00 12:00:00", -1, SQLITE_STATIC);
									handledSpecially = true;
								}
							}

							if (!handledSpecially) {
								// Check whether we have to convert '(-)infinity' to SQLite's understanding of Inf / -Inf.
								// 9e999 will be stored as comparable Inf / -Inf value, but is not ok for dates,
								// corresponding workaround see above.
								if (strcmp(plainValue, "infinity") == 0) {
									ret2 = sqlite3_bind_text(insertStmt, j + 1, "9e999", -1, SQLITE_STATIC);
									handledSpecially = true;
								} else if (strcmp(plainValue, "-infinity") == 0) {
									ret2 = sqlite3_bind_text(insertStmt, j + 1, "-9e999", -1, SQLITE_STATIC);
									handledSpecially = true;
								}
							}

							// Finally, the normal case :-)
							if (!handledSpecially) {
								if (fieldIsNull) {
									ret2 = sqlite3_bind_null(insertStmt, j + 1);
								} else {
									ret2 = sqlite3_bind_text(insertStmt, j + 1, plainValue, -1, SQLITE_STATIC);
								}
							}
						}
						if (ret2 != SQLITE_OK) {
							std::cerr << "Error binding values to insert-query, error code " << ret2 << "!" << std::endl;
							std::cerr << sqlite3_errmsg(sqliteDB) << std::endl;
							return -1;
						}
					}
					int ret3 = sqlite3_step(insertStmt);
					if (ret3 != SQLITE_DONE) {
						std::cerr << "Error inserting values into SQLite, error code " << ret3 << "!" << std::endl;
						std::cerr << sqlite3_errmsg(sqliteDB) << std::endl;
						return -1;
					}
					sqlite3_reset(insertStmt);

					if (i % 1000 == 0) {
						// give some feedback on long waiting times
						std::cout << "inserting row " << i + 1 << "/" << rowCount;
						printf("\r");
						fflush(stdout);
					}

					// For large tables, force commit to SQLite all 100000 rows:
					if ((rowCount > 100000) && (i % 100000 == 0)) {
						endSQLiteTransaction(sqliteDB);
						beginSQLiteTransaction(sqliteDB);
					}

				}
				PQclear(res3);

				endPGSQLTransaction(dbc);
			}

			if (insertStmt != nullptr) {
				sqlite3_finalize(insertStmt);
			}
		}
	} else {
		std::cout << std::endl;
		std::cerr << "Something failed with table-name-selection-query!" << std::endl;
		std::cerr << "Exiting now!" << std::endl;
		exit(1);
	}

	PQclear(res);

	dropLOsizeFun(dbc);
	PQfinish(dbc);

	// End the transaction, reenables autocommit
	endSQLiteTransaction(sqliteDB);

	{
		std::cout << "Running 'ANALYZE;' on fresh SQLite DB to help query-planner... ";
		char *sqlErrorMsg;
		sqlite3_exec(sqliteDB, "ANALYZE;", nullptr, nullptr, &sqlErrorMsg);
		if (sqlErrorMsg != nullptr) {
			std::cout << std::endl;
			std::cerr << std::setw(10) << "" << "Error running 'ANALYZE;'!" << std::endl;
			std::cerr << std::setw(10) << "" << sqlErrorMsg << std::endl;
			std::cerr << std::setw(10) << "" << "Ignoring..." << std::endl;
		} else {
			std::cout << "Done!" << std::endl;
		}
		sqlite3_free(sqlErrorMsg);
	}

	sqlite3_close(sqliteDB);

	std::cout << "Successfully saved SQLite-database to '" << sqliteFilename << "'." << std::endl;

	{
		std::string currentWorkDir;
		{
			char cwdTemp[PATH_MAX];
			currentWorkDir = getcwd(cwdTemp, PATH_MAX) ? std::string(cwdTemp) : std::string("");
		}
		std::cout << std::endl;
		std::cout << "SQLite database created at: " << currentWorkDir.c_str() << "/" << sqliteFilename << std::endl;
		std::cout << std::endl;
	}

	return 0;
}
