package mysqldump

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"text/template"
	"time"
)

/*
Data struct to configure dump behavior

    Out:          Stream to wite to
    Connection:   Database connection to dump
    IgnoreTables: Mark sensitive tables to ignore
    LockTables:   Lock all tables for the duration of the dump
*/
type Data struct {
	Out              io.Writer
	Connection       *sql.DB
	IgnoreTables     []string
	MaxAllowedPacket int
	LockTables       bool

	tx         *sql.Tx
	headerTmpl *template.Template
	tableTmpl  *template.Template
	footerTmpl *template.Template
	err        error
}

type table struct {
	Name string
	Err  error

	data   *Data
	rows   *sql.Rows
	types  []reflect.Type
	values []interface{}
}

type metaData struct {
	DumpVersion   string
	ServerVersion string
	CompleteTime  string
}

const (
	// Version of this plugin for easy reference
	Version = "0.5.0"

	defaultMaxAllowedPacket = 4194304
)

// takes a *metaData
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

// takes a *metaData
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

// Takes a *table
const tableTmpl = `
--
-- Table structure for table {{ .NameEsc }}
--

DROP TABLE IF EXISTS {{ .NameEsc }};
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
{{ .CreateSQL }};
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table {{ .NameEsc }}
--

LOCK TABLES {{ .NameEsc }} WRITE;
/*!40000 ALTER TABLE {{ .NameEsc }} DISABLE KEYS */;
{{ range $value := .Stream }}
{{- $value }}
{{ end -}}
/*!40000 ALTER TABLE {{ .NameEsc }} ENABLE KEYS */;
UNLOCK TABLES;
`

const nullType = "NULL"

// Dump data using struct
func (data *Data) Dump() error {
	meta := metaData{
		DumpVersion: Version,
	}

	if data.MaxAllowedPacket == 0 {
		data.MaxAllowedPacket = defaultMaxAllowedPacket
	}

	if err := data.getTemplates(); err != nil {
		return err
	}

	if err := data.begin(); err != nil {
		return err
	}
	defer data.rollback()

	if err := meta.updateServerVersion(data); err != nil {
		return err
	}

	if err := data.headerTmpl.Execute(data.Out, meta); err != nil {
		return err
	}

	tables, err := data.getTables()
	if err != nil {
		return err
	}

	// Lock all tables before dumping if present
	if data.LockTables && len(tables) > 0 {
		var b bytes.Buffer
		b.WriteString("LOCK TABLES ")
		for index, name := range tables {
			if index != 0 {
				b.WriteString(",")
			}
			b.WriteString("`" + name + "` READ /*!32311 LOCAL */")
		}

		if _, err := data.Connection.Exec(b.String()); err != nil {
			return err
		}

		defer data.Connection.Exec("UNLOCK TABLES")
	}

	for _, name := range tables {
		if err := data.dumpTable(name); err != nil {
			return err
		}
	}
	if data.err != nil {
		return data.err
	}

	meta.CompleteTime = time.Now().String()
	return data.footerTmpl.Execute(data.Out, meta)
}

// MARK: - Private methods

func (data *Data) begin() (err error) {
	data.tx, err = data.Connection.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	return
}

func (data *Data) rollback() error {
	return data.tx.Rollback()
}

// MARK: writter methods

func (data *Data) dumpTable(name string) error {
	if data.err != nil {
		return data.err
	}
	table := data.createTable(name)
	return data.writeTable(table)
}

func (data *Data) writeTable(table *table) error {
	if err := data.tableTmpl.Execute(data.Out, table); err != nil {
		return err
	}
	return table.Err
}

// MARK: get methods

// getTemplates initilaizes the templates on data from the constants in this file
func (data *Data) getTemplates() (err error) {
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

	rows, err := data.tx.Query("SHOW TABLES")
	if err != nil {
		return tables, err
	}
	defer rows.Close()

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

func (meta *metaData) updateServerVersion(data *Data) (err error) {
	var serverVersion sql.NullString
	err = data.tx.QueryRow("SELECT version()").Scan(&serverVersion)
	meta.ServerVersion = serverVersion.String
	return
}

// MARK: create methods

func (data *Data) createTable(name string) *table {
	return &table{
		Name: name,
		data: data,
	}
}

func (table *table) NameEsc() string {
	return "`" + table.Name + "`"
}

func (table *table) CreateSQL() (string, error) {
	var tableReturn, tableSQL sql.NullString
	if err := table.data.tx.QueryRow("SHOW CREATE TABLE "+table.NameEsc()).Scan(&tableReturn, &tableSQL); err != nil {
		return "", err
	}

	if tableReturn.String != table.Name {
		return "", errors.New("Returned table is not the same as requested table")
	}

	return tableSQL.String, nil
}

// defer rows.Close()
func (table *table) Init() (err error) {
	if len(table.types) != 0 {
		return errors.New("can't init twice")
	}

	table.rows, err = table.data.tx.Query("SELECT * FROM " + table.NameEsc())
	if err != nil {
		return err
	}

	columns, err := table.rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("No columns in table " + table.Name + ".")
	}

	tt, err := table.rows.ColumnTypes()
	if err != nil {
		return err
	}

	table.types = make([]reflect.Type, len(tt))
	for i, tp := range tt {
		st := tp.ScanType()
		if tp.DatabaseTypeName() == "BLOB" {
			table.types[i] = reflect.TypeOf(sql.RawBytes{})
		} else if st != nil && (st.Kind() == reflect.Int ||
			st.Kind() == reflect.Int8 ||
			st.Kind() == reflect.Int16 ||
			st.Kind() == reflect.Int32 ||
			st.Kind() == reflect.Int64) {
			table.types[i] = reflect.TypeOf(sql.NullInt64{})
		} else {
			table.types[i] = reflect.TypeOf(sql.NullString{})
		}
	}
	table.values = make([]interface{}, len(tt))
	for i := range table.values {
		table.values[i] = reflect.New(table.types[i]).Interface()
	}
	return nil
}

func (table *table) Next() bool {
	if table.rows == nil {
		if err := table.Init(); err != nil {
			table.Err = err
			return false
		}
	}
	// Fallthrough
	if table.rows.Next() {
		if err := table.rows.Scan(table.values...); err != nil {
			table.Err = err
			return false
		} else if err := table.rows.Err(); err != nil {
			table.Err = err
			return false
		}
	} else {
		table.rows.Close()
		table.rows = nil
		return false
	}
	return true
}

func (table *table) RowValues() string {
	return table.RowBuffer().String()
}

func (table *table) RowBuffer() *bytes.Buffer {
	var b bytes.Buffer
	b.WriteString("(")

	for key, value := range table.values {
		if key != 0 {
			b.WriteString(",")
		}
		switch s := value.(type) {
		case nil:
			b.WriteString(nullType)
		case *sql.NullString:
			if s.Valid {
				fmt.Fprintf(&b, "'%s'", sanitize(s.String))
			} else {
				b.WriteString(nullType)
			}
		case *sql.NullInt64:
			if s.Valid {
				fmt.Fprintf(&b, "%d", s.Int64)
			} else {
				b.WriteString(nullType)
			}
		case *sql.RawBytes:
			if len(*s) == 0 {
				b.WriteString(nullType)
			} else {
				fmt.Fprintf(&b, "_binary '%s'", sanitize(string(*s)))
			}
		default:
			fmt.Fprintf(&b, "'%s'", value)
		}
	}
	b.WriteString(")")

	return &b
}

func (table *table) Stream() <-chan string {
	valueOut := make(chan string, 1)
	go func() {
		defer close(valueOut)
		var insert bytes.Buffer

		for table.Next() {
			b := table.RowBuffer()
			// Truncate our insert if it won't fit
			if insert.Len() != 0 && insert.Len()+b.Len() > table.data.MaxAllowedPacket-1 {
				insert.WriteString(";")
				valueOut <- insert.String()
				insert.Reset()
			}

			if insert.Len() == 0 {
				fmt.Fprintf(&insert, "INSERT INTO %s VALUES ", table.NameEsc())
			} else {
				insert.WriteString(",")
			}
			b.WriteTo(&insert)
		}
		if insert.Len() != 0 {
			insert.WriteString(";")
			valueOut <- insert.String()
		}
	}()
	return valueOut
}
