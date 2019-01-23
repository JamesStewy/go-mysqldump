package mysqldump

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
)

type table struct {
	Name   string
	SQL    string
	Values string
}

type dump struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
	HeaderTmpl    *template.Template
	TableTmpl     *template.Template
	FooterTmpl    *template.Template
	Connection    *sql.DB
	Out           io.Writer
}

const version = "0.3.1"

const headerTmpl = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8mb4;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
`

const tableTmpl = `
--
-- Table structure for table {{ .Name }}
--

DROP TABLE IF EXISTS ` + "`{{ .Name }}`" + `;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4;
{{ .SQL }};
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table {{ .Name }}
--

LOCK TABLES {{ .Name }} WRITE;
/*!40000 ALTER TABLE {{ .Name }} DISABLE KEYS */;
{{- if .Values }}
INSERT INTO {{ .Name }} VALUES {{ .Values }};
{{- end }}
/*!40000 ALTER TABLE {{ .Name }} ENABLE KEYS */;
UNLOCK TABLES;
`

const footerTmpl = `
-- Dump completed on {{ .CompleteTime }}
`

func (data *dump) getTemplates() (err error) {
	// Write dump to file
	data.HeaderTmpl, err = template.New("mysqldumpHeader").Parse(headerTmpl)
	if err != nil {
		return
	}

	data.TableTmpl, err = template.New("mysqldumpTable").Parse(tableTmpl)
	if err != nil {
		return
	}

	data.FooterTmpl, err = template.New("mysqldumpTable").Parse(footerTmpl)
	if err != nil {
		return
	}
	return
}

func (data *dump) dump() error {
	if err := data.HeaderTmpl.Execute(data.Out, data); err != nil {
		return err
	}

	// Get tables
	tables, err := getTables(data.Connection)
	if err != nil {
		return err
	}

	// Get sql for each table
	for _, name := range tables {
		if err := data.dumpTable(name); err != nil {
			return err
		}
	}

	// Set complete time
	data.CompleteTime = time.Now().String()

	if err = data.FooterTmpl.Execute(data.Out, data); err != nil {
		return err
	}

	return nil
}

func (data *dump) dumpTable(name string) error {
	table, err := createTable(data.Connection, name)
	if err != nil {
		return err
	}

	if err = data.TableTmpl.Execute(data.Out, table); err != nil {
		return err
	}

	return nil
}

// Dump creates a MySQL dump based on the options supplied through the dumper.
func (d *Dumper) Dump() (string, error) {
	name := time.Now().Format(d.format)
	p := path.Join(d.dir, name+".sql")

	// Check dump directory
	if e, _ := exists(p); e {
		return p, errors.New("Dump '" + name + "' already exists.")
	}

	// Create .sql file
	f, err := os.Create(p)

	if err != nil {
		return p, err
	}

	defer f.Close()

	return p, Dump(d.db, f)
}

// Dump Creates a MYSQL dump from the connection to the stream.
func Dump(db *sql.DB, out io.Writer) error {
	var err error
	data := dump{
		DumpVersion: version,
		Connection:  db,
		Out:         out,
	}

	// Get server version
	if data.ServerVersion, err = getServerVersion(db); err != nil {
		return err
	}

	if err := data.getTemplates(); err != nil {
		return err
	}

	return data.dump()
}

func getTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

	// Read result
	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return tables, err
		}
		tables = append(tables, table.String)
	}
	return tables, rows.Err()
}

func getServerVersion(db *sql.DB) (string, error) {
	var serverVersion sql.NullString
	if err := db.QueryRow("SELECT version()").Scan(&serverVersion); err != nil {
		return "", err
	}
	return serverVersion.String, nil
}

func createTable(db *sql.DB, name string) (*table, error) {
	var err error
	t := &table{Name: name}

	if t.SQL, err = createTableSQL(db, name); err != nil {
		return nil, err
	}

	if t.Values, err = createTableValues(db, name); err != nil {
		return nil, err
	}

	return t, nil
}

func createTableSQL(db *sql.DB, name string) (string, error) {
	// Get table creation SQL
	var tableReturn, tableSQL sql.NullString
	err := db.QueryRow("SHOW CREATE TABLE "+name).Scan(&tableReturn, &tableSQL)

	if err != nil {
		return "", err
	}
	if tableReturn.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return tableSQL.String, nil
}

func createTableValues(db *sql.DB, name string) (string, error) {
	// Get Data
	rows, err := db.Query("SELECT * FROM " + name)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", errors.New("No columns in table " + name + ".")
	}

	// Read data
	dataText := make([]string, 0)
	for rows.Next() {
		// Init temp data storage

		//ptrs := make([]interface{}, len(columns))
		//var ptrs []interface {} = make([]*sql.NullString, len(columns))

		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = "'" + value.String + "'"
			} else {
				dataStrings[key] = "null"
			}
		}

		dataText = append(dataText, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(dataText, ","), rows.Err()
}
