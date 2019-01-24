package mysqldump

import "strings"

var lazyMySQLReplacer *strings.Replacer

func mysqlReplacer() *strings.Replacer {
	if lazyMySQLReplacer == nil {
		lazyMySQLReplacer = strings.NewReplacer(
			"\x00", "\\0",
			"'", "\\'",
			"\"", "\\\"",
			"\b", "\\b",
			"\n", "\\n",
			"\r", "\\r",
			// "\t", "\\t",
			"\x1A", "\\Z", // ASCII 26 == x1A
			"\\", "\\\\",
			"%", "\\%",
			// "_", "\\_",
		)
	}
	return lazyMySQLReplacer
}

// MySQL sanitizes mysql based on
// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html table 9.1
// needs to be placed in either a single or a double quoted string
func sanitize(input string) string {
	return mysqlReplacer().Replace(input)
}
