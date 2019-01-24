package mysqldump

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
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
	Out           io.Writer
	Connection    *sql.DB

	headerTmpl *template.Template
	tableTmpl  *template.Template
	footerTmpl *template.Template
	mux        sync.Mutex
	wg         sync.WaitGroup
}

const version = "0.3.1"

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

const footerTmpl = `
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on {{ .CompleteTime }}
`

func (data *dump) getTemplates() (err error) {
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

func (data *dump) dump() error {
	if err := data.headerTmpl.Execute(data.Out, data); err != nil {
		return err
	}

	// Get tables
	tables, err := getTables(data.Connection)
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
	data.CompleteTime = time.Now().String()
	return data.footerTmpl.Execute(data.Out, data)
}

func (data *dump) dumpTable(name string) error {
	table, err := createTable(data.Connection, name)
	if err != nil {
		return err
	}

	go data.writeTable(table)
	return nil
}

func (data *dump) writeTable(table *table) error {
	data.mux.Lock()
	err := data.tableTmpl.Execute(data.Out, table)
	data.mux.Unlock()
	data.wg.Done()
	return err
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
	t := &table{Name: "`" + name + "`"}

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
	tt, err := rows.ColumnTypes()
	if err != nil {
		return "", err
	}

	types := make([]reflect.Type, len(tt))
	for i, tp := range tt {
		st := tp.ScanType()
		if st == nil || st.Kind() == reflect.Slice {
			types[i] = reflect.TypeOf(sql.NullString{})
		} else if st.Kind() == reflect.Int ||
			st.Kind() == reflect.Int8 ||
			st.Kind() == reflect.Int16 ||
			st.Kind() == reflect.Int32 ||
			st.Kind() == reflect.Int64 {
			types[i] = reflect.TypeOf(sql.NullInt64{})
		} else {
			types[i] = st
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
				dataStrings[key] = "null"
			} else if s, ok := value.(*sql.NullString); ok {
				if s.Valid {
					dataStrings[key] = "'" + strings.Replace(s.String, "\n", "\\n", -1) + "'"
				} else {
					dataStrings[key] = "NULL"
				}
			} else if s, ok := value.(*sql.NullInt64); ok {
				if s.Valid {
					dataStrings[key] = fmt.Sprintf("%d", s.Int64)
				} else {
					dataStrings[key] = "NULL"
				}
			} else {
				dataStrings[key] = fmt.Sprint("'", value, "'")
			}
		}

		dataText = append(dataText, "("+strings.Join(dataStrings, ",")+")")
	}

	return strings.Join(dataText, ","), rows.Err()
}
