package mysqldump

import (
	"database/sql"
	"errors"
	"os"
)

// Dumper represents a database.
type Dumper struct {
	db     *sql.DB
	format string
	dir    string
}

/*
Creates a new dumper.

	db: Database that will be dumped (https://golang.org/pkg/database/sql/#DB).
	dir: Path to the directory where the dumps will be stored.
	format: Format to be used to name each dump file. Uses time.Time.Format (https://golang.org/pkg/time/#Time.Format). format appended with '.sql'.
*/
func Register(db *sql.DB, dir, format string) (*Dumper, error) {
	if !isDir(dir) {
		return nil, errors.New("Invalid directory")
	}

	return &Dumper{
		db:     db,
		format: format,
		dir:    dir,
	}, nil
}

// Closes the dumper.
// Will also close the database the dumper is connected to.
//
// Not required.
func (d *Dumper) Close() error {
	defer func() {
		d.db = nil
	}()
	return d.db.Close()
}

func exists(p string) (bool, os.FileInfo) {
	f, err := os.Open(p)
	if err != nil {
		return false, nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, nil
	}
	return true, fi
}

func isFile(p string) bool {
	if e, fi := exists(p); e {
		return fi.Mode().IsRegular()
	}
	return false
}

func isDir(p string) bool {
	if e, fi := exists(p); e {
		return fi.Mode().IsDir()
	}
	return false
}
