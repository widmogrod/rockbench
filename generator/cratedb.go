package generator

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewCrateDB(ctx context.Context, url string) (*CrateDB, error) {
	conn, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("cratedb:NewCrateDB: pool; %v", err)
	}

	conn2, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("cratedb:NewCrateDB: single; %v", err)
	}

	return &CrateDB{
		conn:  conn,
		conn2: conn2,
	}, nil
}

var _ Destination = (*CrateDB)(nil)

type ma = map[string]any

// CrateDB contains all configurations needed to send documents to CrateDB
type CrateDB struct {
	conn  *pgxpool.Pool
	conn2 *pgx.Conn
}

func (c *CrateDB) Init(ctx context.Context) error {
	table := `CREATE TABLE IF NOT EXISTS test (
		About STRING,

		Address OBJECT(STRICT) AS (
			City STRING,
			Street STRING,
			ZipCode INTEGER,
			Coordinates OBJECT(STRICT) AS (
				Latitude REAL,
				Longitude REAL
			)
		),

		Age INTEGER,
		Balance REAL,
		Company STRING,
		Email STRING,
		Friends OBJECT(STRICT) AS (
			Friend1 OBJECT(STRICT) AS (
				Name OBJECT(STRICT) AS (
					"First" STRING,
					"Last" STRING
				),
				Age SMALLINT
			),
			Friend2 OBJECT(STRICT) AS (
				Name OBJECT(STRICT) AS (
					"First" STRING,
					"Last" STRING
				),
				Age SMALLINT
			),
			Friend3 OBJECT(STRICT) AS (
				Name OBJECT(STRICT) AS (
					"First" STRING,
					"Last" STRING
				),
				Age SMALLINT
			),
			Friend4 OBJECT(STRICT) AS (
				Name OBJECT(STRICT) AS (
					"First" STRING,
					"Last" STRING
				),
				Age SMALLINT
			),
			Friend5 OBJECT(STRICT) AS (
				Name OBJECT(STRICT) AS (
					"First" STRING,
					"Last" STRING
				),
				Age SMALLINT
			)
		),
		Greeting STRING,
		Guid STRING,
		IsActive BOOLEAN,
		Name OBJECT(STRICT) AS (
			"First" STRING,
			"Last" STRING
		),
		Phone STRING,
		Picture STRING,
		Registered STRING,
		Tags ARRAY(STRING),
		event_time TIMESTAMP WITH TIME ZONE,
		id STRING,
		ts TIMESTAMP WITH TIME ZONE,
		generator_identifier STRING
	);
	`

	_, err := c.conn.Exec(ctx, table)
	if err != nil {
		return fmt.Errorf("cratedb:Init: %v", err)
	}

	return nil
}

func (c *CrateDB) Reset(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, "DROP TABLE IF EXISTS test")
	if err != nil {
		return fmt.Errorf("cratedb:Reset: %v", err)
	}

	return nil
}

func (c *CrateDB) SendDocument(docs []any) error {
	b := &pgx.Batch{}
	for _, doc := range docs {
		insert := `INSERT INTO test (
			About,
			Address,
			Age,
			Balance,
			Company,
			Email,
			Friends,
			Greeting,
			Guid,
			IsActive,
			Name,
			Phone,
			Picture,
			Registered,
			Tags,
			event_time,
			id,
			ts,
			generator_identifier
		) VALUES (
			$1,
			{
				City = $2,
				Coordinates = {
					Latitude = $3::REAL,
					Longitude = $4::REAL
				},
				Street = $5,
				ZipCode = $6::INTEGER
			},
			$7::INTEGER,
			$8::REAL,
			$9,
			$10,
			{
				Friend1 = {
					Name = {
						"First" = $11,
						"Last" = $12
					},
					Age = $13::SMALLINT
				},
				Friend2 = {
					Name = {
						"First" = $14,
						"Last" = $15
					},
					Age = $16::SMALLINT
				},
				Friend3 = {	
					Name = {
						"First" = $17,
						"Last" = $18
					},
					Age = $19::SMALLINT
				},
				Friend4 = {
					Name = {
						"First" = $20,
						"Last" = $21
					},
					Age = $22::SMALLINT
				},
				Friend5 = {
					Name = {
						"First" = $23,
						"Last" = $24
					},
					Age = $25::SMALLINT	
				}
			},
			$26,
			$27,
			$28::BOOLEAN,
			{
				"First" = $29,	
				"Last" = $30
			},
			$31,
			$32,
			$33,
			$34,
			$35,
			$36,
			$37,	
			$38
		);`

		b.Queue(insert,
			doc.(ma)["About"],
			doc.(ma)["Address"].(ma)["City"],
			doc.(ma)["Address"].(ma)["Coordinates"].(ma)["Latitude"],
			doc.(ma)["Address"].(ma)["Coordinates"].(ma)["Longitude"],
			doc.(ma)["Address"].(ma)["Street"],
			doc.(ma)["Address"].(ma)["ZipCode"],
			doc.(ma)["Age"],
			doc.(ma)["Balance"],
			doc.(ma)["Company"],
			doc.(ma)["Email"],
			doc.(ma)["Friends"].(ma)["Friend1"].(ma)["Name"].(ma)["First"],
			doc.(ma)["Friends"].(ma)["Friend1"].(ma)["Name"].(ma)["Last"],
			doc.(ma)["Friends"].(ma)["Friend1"].(ma)["Age"],
			doc.(ma)["Friends"].(ma)["Friend2"].(ma)["Name"].(ma)["First"],
			doc.(ma)["Friends"].(ma)["Friend2"].(ma)["Name"].(ma)["Last"],
			doc.(ma)["Friends"].(ma)["Friend2"].(ma)["Age"],
			doc.(ma)["Friends"].(ma)["Friend3"].(ma)["Name"].(ma)["First"],
			doc.(ma)["Friends"].(ma)["Friend3"].(ma)["Name"].(ma)["Last"],
			doc.(ma)["Friends"].(ma)["Friend3"].(ma)["Age"],
			doc.(ma)["Friends"].(ma)["Friend4"].(ma)["Name"].(ma)["First"],
			doc.(ma)["Friends"].(ma)["Friend4"].(ma)["Name"].(ma)["Last"],
			doc.(ma)["Friends"].(ma)["Friend4"].(ma)["Age"],
			doc.(ma)["Friends"].(ma)["Friend5"].(ma)["Name"].(ma)["First"],
			doc.(ma)["Friends"].(ma)["Friend5"].(ma)["Name"].(ma)["Last"],
			doc.(ma)["Friends"].(ma)["Friend5"].(ma)["Age"],
			doc.(ma)["Greeting"],
			doc.(ma)["Guid"],
			doc.(ma)["IsActive"],
			doc.(ma)["Name"].(ma)["First"],
			doc.(ma)["Name"].(ma)["Last"],
			doc.(ma)["Phone"],
			doc.(ma)["Picture"],
			doc.(ma)["Registered"],
			doc.(ma)["Tags"],
			time.UnixMicro(doc.(ma)["_event_time"].(int64)),
			doc.(ma)["_id"],
			time.UnixMicro(doc.(ma)["_ts"].(int64)),
			doc.(ma)["generator_identifier"],
		)
	}

	br := c.conn.SendBatch(context.Background(), b)
	_, err := br.Exec()
	if err != nil {
		return fmt.Errorf("cratedb:SendDocument: exec; %v", err)
	}

	err = br.Close()
	if err != nil {
		return fmt.Errorf("cratedb:SendDocument: close; %v", err)
	}

	return nil
}

func (c *CrateDB) SendPatch(docs []any) error {
	//TODO implement me
	panic("implement me")
}

func (c *CrateDB) GetLatestTimestamp() (time.Time, error) {
	query := `SELECT MAX(event_time) FROM test.test;`

	var ts time.Time
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: query; %v", err)
	}
	if !rows.Next() {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: no rows")
	}
	err = rows.Scan(&ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: scan; %v", err)
	}
	rows.Close()

	tsv := ts.UnixMicro()

	return time.Unix(tsv/1_000_000, (tsv%1_000_000)*1_000), nil
}

func (c *CrateDB) ConfigureDestination() error {
	//TODO implement me
	panic("implement me")
}

func (c *CrateDB) Close(ctx context.Context) error {
	c.conn.Close()
	return nil
}
