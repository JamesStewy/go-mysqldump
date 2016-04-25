/*
Create MYSQL dumps in Go without the 'mysqldump' CLI as a dependancy.

Example

This example uses the mymysql driver (example 7 https://github.com/ziutek/mymysql) to connect to a mysql instance.

    package main

    import (
    	"database/sql"
    	"fmt"
    	"github.com/JamesStewy/go-mysqldump"
    	"github.com/ziutek/mymysql/godrv"
    	"time"
    )

    func main() {
        // Register the mymysql driver
        godrv.Register("SET NAMES utf8")

        // Open connection to database
        db, err := sql.Open("mymysql", "tcp:host:port*database/user/password")
    	if err != nil {
            fmt.Println("Error opening databse:", err)
            return
        }

        // Register database with mysqldump
        dumper, err := mysqldump.Register(db, "dumps", time.ANSIC)
        if err != nil {
        	fmt.Println("Error registering databse:", err)
        	return
        }

        // Dump database to file
        err = dumper.Dump()
        if err != nil {
        	fmt.Println("Error dumping:", err)
        	return
        }

        // Close dumper and connected database
        dumper.Close()
    }
*/
package mysqldump
