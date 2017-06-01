// Package mongodb provides simple wrapper for mgo.Database object
//
// Example usage:
//
// import "github.com/go-mixins/mongodb"
//
// db := new(mongodb.DB)
// err := db.Connect("mongodb:///database")
// defer db.Close()
// ...
// In some HTTP handler for example:
// ...
// db2 := db.Clone()
// defer db2.Close()
// err = db2.C("collection").Insert(&obj)
// ...
package mongodb

import (
	"time"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

// ConnectTimeout used to limit database connection time
var ConnectTimeout = 10 * time.Second

// DB wraps mgo.Database functionality for ease of use
type DB struct {
	*mgo.Database
}

// Connect creates new database connection from the specified URI
func (db *DB) Connect(DbURI string) (err error) {
	var (
		mongo        *mgo.DialInfo
		mongoSession *mgo.Session
	)

	if mongo, err = mgo.ParseURL(DbURI); err != nil {
		return
	}
	mongo.Timeout = ConnectTimeout
	if mongoSession, err = mgo.DialWithInfo(mongo); err != nil {
		return errors.Wrapf(err, "connecting to %v/%s", mongo.Addrs, mongo.Database)
	}
	mongoSession.SetMode(mgo.Monotonic, true)
	mongoSession.SetSafe(&mgo.Safe{WMode: "majority"})
	db.Database = mongoSession.DB(mongo.Database)
	return
}

// Close closes underlying Session
func (db *DB) Close() {
	db.Session.Close()
}

// Clone clones the database session and returns a new initialized DB object
func (db *DB) Clone() *DB {
	return &DB{db.Session.Clone().DB(db.Name)}
}

// Copy is the same as Clone, but copies the underlying session. See mgo
// documentation for explanation.
func (db *DB) Copy() *DB {
	return &DB{db.Session.Copy().DB(db.Name)}
}
