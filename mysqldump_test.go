package mysqldump

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

const expected = `-- Go SQL Dump ` + Version + `
--
-- ------------------------------------------------------
-- Server version	test_version

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8mb4 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table ~Test_Table~
--

DROP TABLE IF EXISTS ~Test_Table~;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE 'Test_Table' (~id~ int(11) NOT NULL AUTO_INCREMENT,~email~ char(60) DEFAULT NULL, ~name~ char(60), PRIMARY KEY (~id~))ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table ~Test_Table~
--

LOCK TABLES ~Test_Table~ WRITE;
/*!40000 ALTER TABLE ~Test_Table~ DISABLE KEYS */;
INSERT INTO ~Test_Table~ (~id~, ~email~, ~name~) VALUES (1,NULL,'Test Name 1'),(2,'test2@test.de','Test Name 2');
/*!40000 ALTER TABLE ~Test_Table~ ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

`

func mockColumnRows() *sqlmock.Rows {
	var enum struct{}
	col1 := sqlmock.NewColumn("Field").OfType("VARCHAR", "").Nullable(true)
	col2 := sqlmock.NewColumn("Type").OfType("TEXT", "").Nullable(true)
	col3 := sqlmock.NewColumn("Null").OfType("VARCHAR", "").Nullable(true)
	col4 := sqlmock.NewColumn("Key").OfType("ENUM", &enum).Nullable(true)
	col5 := sqlmock.NewColumn("Default").OfType("TEXT", "").Nullable(true)
	col6 := sqlmock.NewColumn("Extra").OfType("VARCHAR", "").Nullable(true)
	return sqlmock.NewRowsWithColumnDefinition(col1, col2, col3, col4, col5, col6).
		AddRow("id", "int(11)", false, nil, 0, "").
		AddRow("email", "varchar(255)", true, nil, nil, "").
		AddRow("name", "varchar(255)", true, nil, nil, "").
		AddRow("hash", "varchar(255)", true, nil, nil, "VIRTUAL GENERATED")
}

func c(name string, v interface{}) *sqlmock.Column {
	var t string
	switch reflect.ValueOf(v).Kind() {
	case reflect.String:
		t = "VARCHAR"
	case reflect.Int:
		t = "INT"
	case reflect.Bool:
		t = "BOOL"
	}
	return sqlmock.NewColumn(name).OfType(t, v).Nullable(true)
}

func RunDump(t testing.TB, data *Data) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	data.Connection = db
	showTablesRows := sqlmock.NewRowsWithColumnDefinition(c("Tables_in_Testdb", "")).
		AddRow("Test_Table")

	showColumnsRows := mockColumnRows()

	serverVersionRows := sqlmock.NewRowsWithColumnDefinition(c("Version()", "")).
		AddRow("test_version")

	createTableRows := sqlmock.NewRowsWithColumnDefinition(c("Table", ""), c("Create Table", "")).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`email` char(60) DEFAULT NULL, `name` char(60), PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueRows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectBegin()
	mock.ExpectQuery(`^SELECT version\(\)$`).WillReturnRows(serverVersionRows)
	mock.ExpectQuery(`^SHOW TABLES$`).WillReturnRows(showTablesRows)
	mock.ExpectExec("^LOCK TABLES `Test_Table` READ /\\*!32311 LOCAL \\*/$").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SHOW COLUMNS FROM `Test_Table`$").WillReturnRows(showColumnsRows)
	mock.ExpectQuery("^SELECT (.+) FROM `Test_Table`$").WillReturnRows(createTableValueRows)
	mock.ExpectRollback()

	assert.NoError(t, data.Dump(), "an error was not expected when dumping a stub database connection")
}

func TestDumpOk(t *testing.T) {
	var buf bytes.Buffer

	RunDump(t, &Data{
		Out:        &buf,
		LockTables: true,
	})

	result := strings.Replace(strings.Split(buf.String(), "-- Dump completed")[0], "`", "~", -1)

	assert.Equal(t, expected, result)
}

func TestNoLockOk(t *testing.T) {
	var buf bytes.Buffer

	data := &Data{
		Out:        &buf,
		LockTables: false,
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	data.Connection = db
	showTablesRows := sqlmock.NewRowsWithColumnDefinition(c("Tables_in_Testdb", "")).
		AddRow("Test_Table")

	showColumnsRows := mockColumnRows()

	serverVersionRows := sqlmock.NewRowsWithColumnDefinition(c("Version()", "")).
		AddRow("test_version")

	createTableRows := sqlmock.NewRowsWithColumnDefinition(c("Table", ""), c("Create Table", "")).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`email` char(60) DEFAULT NULL, `name` char(60), PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueRows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectBegin()
	mock.ExpectQuery(`^SELECT version\(\)$`).WillReturnRows(serverVersionRows)
	mock.ExpectQuery(`^SHOW TABLES$`).WillReturnRows(showTablesRows)
	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SHOW COLUMNS FROM `Test_Table`$").WillReturnRows(showColumnsRows)
	mock.ExpectQuery("^SELECT (.+) FROM `Test_Table`$").WillReturnRows(createTableValueRows)
	mock.ExpectRollback()

	assert.NoError(t, data.Dump(), "an error was not expected when dumping a stub database connection")

	result := strings.Replace(strings.Split(buf.String(), "-- Dump completed")[0], "`", "~", -1)

	assert.Equal(t, expected, result)
}

func BenchmarkDump(b *testing.B) {
	data := &Data{
		Out:        ioutil.Discard,
		LockTables: true,
	}
	for i := 0; i < b.N; i++ {
		RunDump(b, data)
	}
}
