# Go MYSQL Dump
Create MYSQL dumps in Go without the `mysqldump` CLI as a dependancy.

### Simple Example
```go
package main

import (
  "database/sql"
  "fmt"

  "github.com/JamesStewy/go-mysqldump"
  _ "github.com/go-sql-driver/mysql"
)

func main() {
  // Open connection to database
  config := mysql.NewConfig()
  config.User = "your-user"
  config.Passwd = "your-pw"
  config.DBName = "your-db"
  config.Net = "tcp"
  config.Addr = "your-hostname:your-port"

  dumpDir := "dumps"  // you should create this directory
  dumpFilenameFormat := fmt.Sprintf("%s-20060102T150405", dbname)   // accepts time layout string and add .sql at the end of file

  db, err := sql.Open("mysql", config.FormatDNS())
  if err != nil {
    fmt.Println("Error opening database: ", err)
    return
  }

  // Register database with mysqldump
  dumper, err := mysqldump.Register(db, dumpDir, dumpFilenameFormat)
  if err != nil {
    fmt.Println("Error registering databse:", err)
    return
  }

  // Dump database to file
  resultFilename, err := dumper.Dump()
  if err != nil {
    fmt.Println("Error dumping:", err)
    return
  }
  fmt.Printf("File is saved to %s", resultFilename)

  // Close dumper and connected database
  dumper.Close()
}

```

[![GoDoc](https://godoc.org/github.com/JamesStewy/go-mysqldump?status.svg)](https://godoc.org/github.com/JamesStewy/go-mysqldump)
[![Build Status](https://travis-ci.org/JamesStewy/go-mysqldump.svg?branch=master)](https://travis-ci.org/JamesStewy/go-mysqldump)
