package mysqldump

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestDumpOk(t *testing.T) {

	tmpFile := "/tmp/test_format.sql"
	os.Remove(tmpFile)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	showTablesRows := sqlmock.NewRows([]string{"Tables_in_Testdb"}).
		AddRow("Test_Table")

	serverVersionRows := sqlmock.NewRows([]string{"Version()"}).
		AddRow("test_version")

	createTableRows := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("Test_Table", "CREATE TABLE 'Test_Table' (`id` int(11) NOT NULL AUTO_INCREMENT,`email` char(60) DEFAULT NULL, `name` char(60), PRIMARY KEY (`id`))ENGINE=InnoDB DEFAULT CHARSET=latin1")

	createTableValueRows := sqlmock.NewRows([]string{"id", "email", "name"}).
		AddRow(1, nil, "Test Name 1").
		AddRow(2, "test2@test.de", "Test Name 2")

	mock.ExpectQuery("^SELECT version()").WillReturnRows(serverVersionRows)
	mock.ExpectQuery("^SHOW TABLES$").WillReturnRows(showTablesRows)
	mock.ExpectQuery("^SHOW CREATE TABLE Test_Table$").WillReturnRows(createTableRows)
	mock.ExpectQuery("^SELECT (.+) FROM Test_Table$").WillReturnRows(createTableValueRows)

	buf := new(bytes.Buffer)
	err = Dump(db, buf)
	if err != nil {
		t.Fatalf("an error '%s' was not expected when dumping a stub database connection", err)
	}

	result := strings.Replace(strings.Split(buf.String(), "-- Dump completed")[0], "`", "\\", -1)

	expected := `-- Go SQL Dump ` + version + `
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
-- Table structure for table \Test_Table\
--

DROP TABLE IF EXISTS \Test_Table\;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE 'Test_Table' (\id\ int(11) NOT NULL AUTO_INCREMENT,\email\ char(60) DEFAULT NULL, \name\ char(60), PRIMARY KEY (\id\))ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table \Test_Table\
--

LOCK TABLES \Test_Table\ WRITE;
/*!40000 ALTER TABLE \Test_Table\ DISABLE KEYS */;
INSERT INTO \Test_Table\ VALUES (1,null,'Test Name 1'),(2,'test2@test.de','Test Name 2');
/*!40000 ALTER TABLE \Test_Table\ ENABLE KEYS */;
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

	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected %#v, got %#v", expected, result)
	}
}
