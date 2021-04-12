# README
Simple interface to BadgerDB written in Go.

## Key features
1. Provides concurrent access to BadgerDB through mutex locks
2. Simple interface

## Methods
`func NewDB(dbFilePath string) (db *BadgerDB, err error)`
This creates a new interface to BadgerDB. It needs a the path to a file for the db storage. Doesn't return an error if file path is unusable e.g file permission errors etc. Such errors are thrown when you try to set or get (data access methods).

`func (db *BadgerDB) Set(k string, v []byte)`
Save a value, v, for key, k. If k already exists, its value is overwritten, if not a k is created with value v. It returns an error if the db file path is unusable e.g file permission errors etc.

`func (db *BadgerDB) Get(k string) ([]byte, error)`
Get the value, v, for key, k. If k exists, its value is returned, else nil is returned. It returns an error if the db file path is unusable e.g file permission errors etc.
