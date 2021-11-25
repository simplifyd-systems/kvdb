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

	return err
}

// MultiSet []{value v, for key k}
func (db *BadgerDB) MultiSet(ctx context.Context, instr map[string][]byte) error {
	if db.db == nil {
		return fmt.Errorf("DB not connected")
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		for k, v := range instr {
			if err := txn.Set([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})

	return err
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

	return valCopy, err
}

// Del key k
func (db *BadgerDB) Del(ctx context.Context, k string) error {
	if db.db == nil {
		return fmt.Errorf("DB not connected")
	}

	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(k))
		return err
	})
}

// Dump all keys k and values v
func (db *BadgerDB) Dump(ctx context.Context, k string) (map[string][]byte, error) {
	if db.db == nil {
		return nil, fmt.Errorf("DB not connected")
	}

	data := make(map[string][]byte)

	err := db.db.View(func(txn *badger.Txn) error {
		iopt := badger.DefaultIteratorOptions

		itr := txn.NewIterator(iopt)

		defer itr.Close()

		i := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			i++
			key := itr.Item().Key()
			valCopy, err := itr.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			data[string(key)] = valCopy
		}

		return nil
	})

	return data, err
}
