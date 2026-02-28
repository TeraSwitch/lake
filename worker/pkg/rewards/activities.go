package rewards

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Activities holds dependencies for reward simulation activities.
type Activities struct {
	ClickHouse     driver.Conn
	PgPool         *pgxpool.Pool
	ShapleyBinPath string
}
