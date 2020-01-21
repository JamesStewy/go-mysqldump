package mysqldump

import (
	"fmt"
	"testing"
)

func TestForSQLInjection(t *testing.T) {
	examples := [][]string{
		/** Query ** Input ** Expected **/
		{"SELECT * WHERE field = '%s';", "test", "SELECT * WHERE field = 'test';"},
		{"'%s'", "'; DROP TABLES `test`;", "'\\'; DROP TABLES `test`;'"},
		{"'%s'", "'+(SELECT name FROM users LIMIT 1)+'", "'\\'+(SELECT name FROM users LIMIT 1)+\\''"},
		{"SELECT '%s'", "\x00x633A5C626F6F742E696E69", "SELECT '\\0x633A5C626F6F742E696E69'"},
		{"WHERE PASSWORD('%s')", "') OR 1=1--", "WHERE PASSWORD('\\') OR 1=1--')"},
	}
	var query string
	for _, example := range examples {
		query = fmt.Sprintf(example[0], sanitize(example[1]))

		if example[2] != query {
			t.Fatalf("expected %#v, got %#v", example[2], query)
		}
	}
}
