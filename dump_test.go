package mysqldump

import (
	"bytes"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func getMockData() (data *Data, mock sqlmock.Sqlmock, err error) {
	var db *sql.DB
	db, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	mock.ExpectBegin()

	data = &Data{
		Connection: db,
	}
	err = data.begin()
	return
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

func TestGetTablesOk(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow("Test_Table_2")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	result, err := data.getTables()
	assert.NoError(t, err)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	assert.EqualValues(t, []string{"Test_Table_1", "Test_Table_2"}, result)
}

func TestIgnoreTablesOk(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow("Test_Table_2")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	data.IgnoreTables = []string{"Test_Table_1"}

	result, err := data.getTables()
	assert.NoError(t, err)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	assert.EqualValues(t, []string{"Test_Table_2"}, result)
}

func TestGetTablesNil(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow(nil).
		AddRow("Test_Table_3")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	result, err := data.getTables()
	assert.NoError(t, err)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	assert.EqualValues(t, []string{"Test_Table_1", "Test_Table_3"}, result)
}

func TestGetServerVersionOk(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	rows := sqlmock.NewRows([]string{"Version()"}).
		AddRow("test_version")

	mock.ExpectQuery("^SELECT version()").WillReturnRows(rows)

	meta := metaData{}

	assert.NoError(t, meta.updateServerVersion(data), "error was not expected while updating stats")

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	assert.Equal(t, "test_version", meta.ServerVersion)
}

func TestCreateTableSQLOk(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	rows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(rows)

	table := data.createTable("Test_Table")

	result, err := table.CreateSQL()
	assert.NoError(t, err)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	expectedResult := "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1"

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}

func mockTableSelect(mock sqlmock.Sqlmock, name string) {
	cols := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("email", "").
		AddRow("name", "")

	rows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, "test@test.de", "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SHOW COLUMNS FROM `" + name + "`$").WillReturnRows(cols)
	mock.ExpectQuery("^SELECT (.+) FROM `" + name + "`$").WillReturnRows(rows)
}

func TestCreateTableRowValues(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	mockTableSelect(mock, "test")

	table := data.createTable("test")

	assert.True(t, table.Next())

	result := table.RowValues()
	assert.NoError(t, table.Err)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	assert.EqualValues(t, "(1,'test@test.de','Test Name 1')", result)
}

func TestCreateTableValuesSteam(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	mockTableSelect(mock, "test")

	data.MaxAllowedPacket = 4096

	table := data.createTable("test")

	s := table.Stream()
	assert.EqualValues(t, "INSERT INTO `test` (`id`, `email`, `name`) VALUES (1,'test@test.de','Test Name 1'),(2,'test2@test.de','Test Name 2');", <-s)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")
}

func TestCreateTableValuesSteamSmallPackets(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	mockTableSelect(mock, "test")

	data.MaxAllowedPacket = 64

	table := data.createTable("test")

	s := table.Stream()
	assert.EqualValues(t, "INSERT INTO `test` (`id`, `email`, `name`) VALUES (1,'test@test.de','Test Name 1');", <-s)
	assert.EqualValues(t, "INSERT INTO `test` (`id`, `email`, `name`) VALUES (2,'test2@test.de','Test Name 2');", <-s)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")
}

func TestCreateTableAllValuesWithNil(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	cols := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("email", "").
		AddRow("name", "")

	rows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2").
		AddRow(3, "", "Test Name 3")

	mock.ExpectQuery("^SHOW COLUMNS FROM `test`$").WillReturnRows(cols)
	mock.ExpectQuery("^SELECT (.+) FROM `test`$").WillReturnRows(rows)

	table := data.createTable("test")

	results := make([]string, 0)
	for table.Next() {
		row := table.RowValues()
		assert.NoError(t, table.Err)
		results = append(results, row)
	}

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	expectedResults := []string{"(1,NULL,'Test Name 1')", "(2,'test2@test.de','Test Name 2')", "(3,'','Test Name 3')"}

	assert.EqualValues(t, expectedResults, results)
}

func TestCreateTableOk(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	createTableRows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueCols := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("email", "").
		AddRow("name", "")

	createTableValueRows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SHOW COLUMNS FROM `Test_Table`$").WillReturnRows(createTableValueCols)
	mock.ExpectQuery("^SELECT (.+) FROM `Test_Table`$").WillReturnRows(createTableValueRows)

	var buf bytes.Buffer
	data.Out = &buf
	data.MaxAllowedPacket = 4096

	assert.NoError(t, data.getTemplates())

	table := data.createTable("Test_Table")

	data.writeTable(table)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	expectedResult := `
--
-- Table structure for table ~Test_Table~
--

DROP TABLE IF EXISTS ~Test_Table~;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE 'Test_Table' (~id~ int(11) NOT NULL AUTO_INCREMENT,~s~ char(60) DEFAULT NULL, PRIMARY KEY (~id~))ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table ~Test_Table~
--

LOCK TABLES ~Test_Table~ WRITE;
/*!40000 ALTER TABLE ~Test_Table~ DISABLE KEYS */;
INSERT INTO ~Test_Table~ (~id~, ~email~, ~name~) VALUES (1,NULL,'Test Name 1'),(2,'test2@test.de','Test Name 2');
/*!40000 ALTER TABLE ~Test_Table~ ENABLE KEYS */;
UNLOCK TABLES;
`
	result := strings.Replace(buf.String(), "`", "~", -1)
	assert.Equal(t, expectedResult, result)
}

func TestCreateTableOkSmallPackets(t *testing.T) {
	data, mock, err := getMockData()
	assert.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer data.Close()

	createTableRows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueCols := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("email", "").
		AddRow("name", "")

	createTableValueRows := sqlmock.NewRowsWithColumnDefinition(c("id", 0), c("email", ""), c("name", "")).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SHOW COLUMNS FROM `Test_Table`$").WillReturnRows(createTableValueCols)
	mock.ExpectQuery("^SELECT (.+) FROM `Test_Table`$").WillReturnRows(createTableValueRows)

	var buf bytes.Buffer
	data.Out = &buf
	data.MaxAllowedPacket = 64

	assert.NoError(t, data.getTemplates())

	table := data.createTable("Test_Table")

	data.writeTable(table)

	// we make sure that all expectations were met
	assert.NoError(t, mock.ExpectationsWereMet(), "there were unfulfilled expections")

	expectedResult := `
--
-- Table structure for table ~Test_Table~
--

DROP TABLE IF EXISTS ~Test_Table~;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE 'Test_Table' (~id~ int(11) NOT NULL AUTO_INCREMENT,~s~ char(60) DEFAULT NULL, PRIMARY KEY (~id~))ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table ~Test_Table~
--

LOCK TABLES ~Test_Table~ WRITE;
/*!40000 ALTER TABLE ~Test_Table~ DISABLE KEYS */;
INSERT INTO ~Test_Table~ (~id~, ~email~, ~name~) VALUES (1,NULL,'Test Name 1');
INSERT INTO ~Test_Table~ (~id~, ~email~, ~name~) VALUES (2,'test2@test.de','Test Name 2');
/*!40000 ALTER TABLE ~Test_Table~ ENABLE KEYS */;
UNLOCK TABLES;
`
	result := strings.Replace(buf.String(), "`", "~", -1)
	assert.Equal(t, expectedResult, result)
}
