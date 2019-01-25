package mysqldump

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"text/template"
	"time"
)

/*
Data struct to configure dump behavior

    Out:          Stream to wite to
    Connection:   Database connection to dump
    IgnoreTables: Mark sensitive tables to ignore
*/
type Data struct {
	Out          io.Writer
	Connection   *sql.DB
	IgnoreTables []string

	headerTmpl *template.Template
	tableTmpl  *template.Template
	footerTmpl *template.Template
	mux        sync.Mutex
	wg         sync.WaitGroup
}

type table struct {
	Name   string
	SQL    string
	Values string
}

type metaData struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
}

const version = "0.3.3"

const headerTmpl = `-- Go SQL Dump {{ .DumpVersion }}
--
-- ------------------------------------------------------
-- Server version	{{ .ServerVersion }}

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
`

const tableTmpl = `
--
-- Table structure for table {{ .Name }}
--

DROP TABLE IF EXISTS {{ .Name }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
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

const footerTmpl = `/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on {{ .CompleteTime }}
`

// Dump data using struct
func (data *Data) Dump() error {
	meta := metaData{
		DumpVersion: version,
	}

	// Get server version
	if err := meta.updateServerVersion(data.Connection); err != nil {
		return err
	}

	if err := data.getTemplates(); err != nil {
		return err
	}

	if err := data.headerTmpl.Execute(data.Out, meta); err != nil {
		return err
	}

	// Get tables
	tables, err := data.getTables()
	if err != nil {
		return err
	}

	// Get sql for each table
	data.wg.Add(len(tables))
	for _, name := range tables {
		if err := data.dumpTable(name); err != nil {
			return err
		}
	}
	data.wg.Wait()

	// Set complete time
	meta.CompleteTime = time.Now().String()
	return data.footerTmpl.Execute(data.Out, meta)
}

// MARK: - Private methods

// MARK: writter methods

func (data *Data) dumpTable(name string) error {
	table, err := data.createTable(name)
	if err != nil {
		return err
	}

	go data.writeTable(table)
	return nil
}

func (data *Data) writeTable(table *table) error {
	data.mux.Lock()
	err := data.tableTmpl.Execute(data.Out, table)
	data.mux.Unlock()
	data.wg.Done()
	return err
}

// MARK: get methods

func (data *Data) getTemplates() (err error) {
	// Write dump to file
	data.headerTmpl, err = template.New("mysqldumpHeader").Parse(headerTmpl)
	if err != nil {
		return
	}

	data.tableTmpl, err = template.New("mysqldumpTable").Parse(tableTmpl)
	if err != nil {
		return
	}

	data.footerTmpl, err = template.New("mysqldumpTable").Parse(footerTmpl)
	if err != nil {
		return
	}
	return
}

func (data *Data) getTables() ([]string, error) {
	tables := make([]string, 0)

	// Get table list
	rows, err := data.Connection.Query("SHOW TABLES")
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
		if table.Valid && !data.isIgnoredTable(table.String) {
			tables = append(tables, table.String)
		}
	}
	return tables, rows.Err()
}

func (data *Data) isIgnoredTable(name string) bool {
	for _, item := range data.IgnoreTables {
		if item == name {
			return true
		}
	}
	return false
}

func (data *metaData) updateServerVersion(db *sql.DB) (err error) {
	var serverVersion sql.NullString
	err = db.QueryRow("SELECT version()").Scan(&serverVersion)
	data.ServerVersion = serverVersion.String
	return
}

// MARK: create methods

func (data *Data) createTable(name string) (*table, error) {
	var err error
	t := &table{Name: "`" + name + "`"}

	if t.SQL, err = data.createTableSQL(name); err != nil {
		return nil, err
	}

	if t.Values, err = data.createTableValues(name); err != nil {
		return nil, err
	}

	return t, nil
}

func (data *Data) createTableSQL(name string) (string, error) {
	// Get table creation SQL
	var tableReturn, tableSQL sql.NullString
	err := data.Connection.QueryRow("SHOW CREATE TABLE "+name).Scan(&tableReturn, &tableSQL)

	if err != nil {
		return "", err
	}
	if tableReturn.String != name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return tableSQL.String, nil
}

func (data *Data) createTableValues(name string) (string, error) {
	// Get Data
	rows, err := data.Connection.Query("SELECT * FROM " + name)
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
	tt, err := rows.ColumnTypes()
	if err != nil {
		return "", err
	}

	types := make([]reflect.Type, len(tt))
	for i, tp := range tt {
		st := tp.ScanType()
		if tp.DatabaseTypeName() == "BLOB" {
			types[i] = reflect.TypeOf(sql.RawBytes{})
		} else if st != nil && (st.Kind() == reflect.Int ||
			st.Kind() == reflect.Int8 ||
			st.Kind() == reflect.Int16 ||
			st.Kind() == reflect.Int32 ||
			st.Kind() == reflect.Int64) {
			types[i] = reflect.TypeOf(sql.NullInt64{})
		} else {
			types[i] = reflect.TypeOf(sql.NullString{})
		}
	}
	values := make([]interface{}, len(tt))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}
	for rows.Next() {
		// Read data
		if err := rows.Scan(values...); err != nil {
			return "", err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range values {
			if value == nil {
				dataStrings[key] = "NULL"
			} else if s, ok := value.(*sql.NullString); ok {
				if s.Valid {
					dataStrings[key] = "'" + sanitize(s.String) + "'"
				} else {
					dataStrings[key] = "NULL"
				}
			} else if s, ok := value.(*sql.NullInt64); ok {
				if s.Valid {
					dataStrings[key] = fmt.Sprintf("%d", s.Int64)
				} else {
					dataStrings[key] = "NULL"
				}
			} else if s, ok := value.(*sql.RawBytes); ok {
				if len(*s) == 0 {
					dataStrings[key] = "NULL"
				} else {
					dataStrings[key] = "_binary '" + sanitize(string(*s)) + "'"
				}
			} else {
				dataStrings[key] = fmt.Sprint("'", value, "'")
			}
		}

		dataText = append(dataText, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(dataText, ","), rows.Err()
}
