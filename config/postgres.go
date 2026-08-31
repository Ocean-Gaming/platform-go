package config

import (
	"context"
	"database/sql"
	"encoding/json"
)

// PostgresLoader reads the whole tenant config snapshot in one query.
//
// Called on boot and on ConfigChanged - never per request. Rule 7 exists so a
// config read is a map lookup, not a network call.
type PostgresLoader struct{ DB *sql.DB }

func (l PostgresLoader) Load(ctx context.Context) (map[string]TenantConfig, error) {
	rows, err := l.DB.QueryContext(ctx,
		`SELECT tenant_id, version, document FROM tenant_config_snapshot`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]TenantConfig{}
	for rows.Next() {
		var (
			id  string
			ver int64
			doc []byte
		)
		if err := rows.Scan(&id, &ver, &doc); err != nil {
			return nil, err
		}
		var c TenantConfig
		if len(doc) > 0 {
			if err := json.Unmarshal(doc, &c); err != nil {
				return nil, err
			}
		}
		c.TenantID, c.Version = id, ver
		out[id] = c
	}
	return out, rows.Err()
}
