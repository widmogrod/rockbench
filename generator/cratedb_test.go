package generator

import (
	"context"
	"testing"
)

func crateDbDocs() ([]any, error) {
	spec := DocumentSpec{
		Destination:          "elastic",
		GeneratorIdentifier:  "1",
		BatchSize:            10,
		Mode:                 "add",
		IdMode:               "sequential",
		UpdatePercentage:     -1,
		NumClusters:          -1,
		HotClusterPercentage: -1,
	}

	docs, err := GenerateDocs(spec)
	return docs, err
}

func TestNewCrateDB(t *testing.T) {
	uri := "postgres://crate:@localhost:5432/test"
	c, err := NewCrateDB(context.Background(), uri)
	if err != nil {
		t.Fatalf("NewCrateDB() error = %v", err)
	}

	if c == nil {
		t.Fatalf("NewCrateDB() = nil")
	}

	err = c.Reset(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Reset() error = %v", err)
	}

	err = c.Init(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Init() error = %v", err)
	}

	docs, err := crateDbDocs()
	if err != nil {
		t.Fatalf("crateDbDocs() error = %v", err)
	}

	err = c.SendDocument(docs)
	if err != nil {
		t.Fatalf("CrateDB.SendDocument() error = %v", err)
	}

	err = c.Reset(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Reset() error = %v", err)
	}

	err = c.Close(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Close() error = %v", err)
	}
}
