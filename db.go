package kvdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
)

var ErrDBNotCoonected = errors.New("db not connected")

// ErrMongoDBDuplicate error
var ErrKVDBDuplicate = errors.New("duplicate entry")

// ErrNotFound error
var ErrNotFound = errors.New("item not found")

// BadgerDB connection holder
type BadgerDB struct {
	db   *badger.DB
	path string
}

// NewDB creates a DB connection and returns a db instance
func NewDB(dbFilePath string) (db *BadgerDB, err error) {
	db = &BadgerDB{}

	db.path = dbFilePath

	db.db, err = badger.Open(badger.DefaultOptions(db.path))
	if err != nil {
		return nil, err
	}

	return
}

// Disconnect closes the connection to the db and releases the lock
func (db *BadgerDB) Disconnect() {
	db.db.Close()
}

// GetClient func
func (db *BadgerDB) GetClient() *badger.DB {
	return db.db
}

// Set value v, for key k
func (db *BadgerDB) Set(ctx context.Context, k string, v []byte) error {
	if db.db == nil {
		return fmt.Errorf("DB not connected")
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(k), v)
		return err
	})

	if err != nil {
		return err
	}

	return nil
}

// Get value for key k
func (db *BadgerDB) Get(ctx context.Context, k string) ([]byte, error) {
	if db.db == nil {
		return nil, fmt.Errorf("DB not connected")
	}

	var valCopy []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}

		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return valCopy, nil
}
