package mysqldump

import (
	"reflect"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestGetTablesOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow("Test_Table_2")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	data := Data{
		Connection: db,
	}

	result, err := data.getTables()
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := []string{"Test_Table_1", "Test_Table_2"}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", result, expectedResult)
	}
}

func TestIgnoreTablesOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow("Test_Table_2")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	data := Data{
		Connection:   db,
		IgnoreTables: []string{"Test_Table_1"},
	}

	result, err := data.getTables()
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := []string{"Test_Table_2"}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", result, expectedResult)
	}
}

func TestGetTablesNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table_1").
		AddRow(nil).
		AddRow("Test_Table_3")

	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(rows)

	data := Data{
		Connection: db,
	}

	result, err := data.getTables()
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := []string{"Test_Table_1", "Test_Table_3"}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestGetServerVersionOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"Version()"}).
		AddRow("test_version")

	mock.ExpectQuery("^SELECT version()").WillReturnRows(rows)

	meta := metaData{}

	if err := meta.updateServerVersion(db); err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := "test_version"

	if !reflect.DeepEqual(meta.ServerVersion, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, meta.ServerVersion)
	}
}

func TestCreateTableSQLOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(rows)

	data := Data{
		Connection: db,
	}

	result, err := data.createTableSQL("Test_Table")

	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1"

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestCreateTableValuesOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "email", "name"}).
		AddRow(1, "test@test.de", "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SELECT (.+) FROM `test`$").WillReturnRows(rows)

	data := Data{
		Connection: db,
	}

	result, err := data.createTableValues("test")
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := []string{"('1','test@test.de','Test Name 1')", "('2','test2@test.de','Test Name 2')"}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestCreateTableValuesNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "email", "name"}).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2").
		AddRow(3, "", "Test Name 3")

	mock.ExpectQuery("^SELECT (.+) FROM `test`$").WillReturnRows(rows)

	data := Data{
		Connection: db,
	}

	result, err := data.createTableValues("test")
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := []string{"('1',NULL,'Test Name 1')", "('2','test2@test.de','Test Name 2')", "('3','','Test Name 3')"}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}

func TestCreateTableOk(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	createTableRows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueRows := sqlmock.NewRows([]string{"id", "email", "name"}).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SHOW CREATE TABLE `Test_Table`$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SELECT (.+) FROM `Test_Table`$").WillReturnRows(createTableValueRows)

	data := Data{
		Connection: db,
	}

	result, err := data.createTable("Test_Table")
	if err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	expectedResult := &table{
		Name:   "`Test_Table`",
		SQL:    "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`s` char(60) DEFAULT NULL, PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1",
		Values: []string{"('1',NULL,'Test Name 1')", "('2','test2@test.de','Test Name 2')"},
	}

	if !reflect.DeepEqual(result, expectedResult) {
		t.Fatalf("expected %#v, got %#v", expectedResult, result)
	}
}
