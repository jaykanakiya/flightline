// Package clickhouse wraps the official ClickHouse driver for the inserts
// and stats queries this project needs.
package clickhouse

import "github.com/ClickHouse/clickhouse-go/v2"

func NewConn(addr, database, username, password string) (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	})
}
