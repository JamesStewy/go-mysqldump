package mysqldump_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jamf/go-mysqldump"
)

var socketFiles = []string{
	"/tmp/mysql.sock",
	"/var/run/mysqld/mysqld.sock",
	"/var/lib/mysql/mysql.sock",
}

func findSocketFile() string {
	// Search under known defaults
	for _, socket := range socketFiles {
		if _, err := os.Stat(socket); err == nil {
			fmt.Println("default mysqld socket found:", socket)
			return socket
		}
	}
	return socketFiles[0]
}

func TestDumpMyLocal(t *testing.T) {
	config := mysql.NewConfig()
	config.MaxAllowedPacket = 0
	config.DBName = "jamfsoftware"
	config.User = "jamfsoftware"
	config.Passwd = "jamfsw03"
	config.Net = "unix"
	config.Addr = findSocketFile()

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Error(err)
	}

	dump := &mysqldump.Data{
		Out:        os.Stdout,
		Connection: db,
	}

	if err := dump.Dump(); err != nil {
		t.Error(err)
	}
}
