package kvdb

import (
	"errors"
	"log"
	"sync"

	"github.com/dgraph-io/badger"
)

// ErrMongoDBDuplicate error
var ErrKVDBDuplicate = errors.New("Duplicate entry")

// ErrNotFound error
var ErrNotFound = errors.New("Item not found")

// BadgerDB connection holder
type BadgerDB struct {
	db   *badger.DB
	path string
	lock sync.Mutex
}

// NewDB creates a DB connection and returns a db instance
func NewDB(dbFilePath string) (db *BadgerDB, err error) {
	db = &BadgerDB{}

	db.path = dbFilePath
	return
}

// disconnect closes the connection to the db and releases the lock
func (db *BadgerDB) disconnect() {
	db.db.Close()
	db.lock.Unlock()
}

// GetClient func
func (db *BadgerDB) GetClient() *badger.DB {
	return db.db
}

func (db *BadgerDB) connectToDB() error {
	var err error

	// hold a lock to access the db
	db.lock.Lock()
	db.db, err = badger.Open(badger.DefaultOptions(db.path))
	if err != nil {
		return err
	}
	return nil
}

// Set value v, for key k
func (db *BadgerDB) Set(k string, v []byte) {
	db.connectToDB()
	defer db.disconnect()

	err := db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(k), v)
		return err
	})

	if err != nil {
		log.Println(err)
	}
}

// Get value for key k
func (db *BadgerDB) Get(k string) ([]byte, error) {
	db.connectToDB()
	defer db.disconnect()

	var valCopy []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}

		// Alternatively, you could also use item.ValueCopy().
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
