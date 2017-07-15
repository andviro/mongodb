// Package mongodb provides simple wrapper for mgo.Database object. It supports
// TLS and x509 auth.
//
// Example usage:
//
// import "github.com/go-mixins/mongodb"
//
//
// db, err := db.New("mongodb:///database", "", "") // No TLS and x509 auth
//
// or
//
// db, err := db.New("mongodb:///database", "/etc/ssl/certs/CA.crt", "/etc/ssl/certs/mongoClient.pem")
//
// defer db.Close()
// ...
// In some transient context
// ...
// db2 := db.Clone()
// defer db2.Close()
//
// err = db2.C("collection").Insert(&obj) // Do some work
// ...
package mongodb

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

func getUsername(cert *x509.Certificate) string {
	var email string
	for _, i := range cert.Subject.Names {
		if i.Type.String() == "1.2.840.113549.1.9.1" {
			email = i.Value.(string)
			break
		}
	}
	return fmt.Sprintf(
		"emailAddress=%s,CN=%s,OU=%s,O=%s,L=%s,C=%s",
		email,
		cert.Subject.CommonName,
		cert.Subject.OrganizationalUnit[0],
		cert.Subject.Organization[0],
		cert.Subject.Locality[0],
		cert.Subject.Country[0],
	)
}

// ConnectTimeout used to limit database connection time
var ConnectTimeout = 10 * time.Second

// DB wraps mgo.Database functionality for ease of use
type DB struct {
	*mgo.Database
}

// New creates new MongoDB connection with optional TLS connection and
// authentication. If caCertFile is non-empty, it specifies server CA
// certificate for server verification and implies TLS connection. If
// pemKeyFile is non-empty it will be presented for client authentication and
// login.
func New(uri, caCertFile, pemKeyFile string) (db *DB, err error) {
	var (
		mongo        *mgo.DialInfo
		mongoSession *mgo.Session
		clientCert   tls.Certificate
	)

	if mongo, err = mgo.ParseURL(uri); err != nil {
		return
	}

	if pemKeyFile != "" {
		if caCertFile == "" {
			err = errors.New("pemKeyFile specified without caCertFile")
			return
		}
		if clientCert, err = tls.LoadX509KeyPair(pemKeyFile, pemKeyFile); err != nil {
			err = errors.Wrap(err, "loading CA certificate")
			return
		}
		if clientCert.Leaf, err = x509.ParseCertificate(clientCert.Certificate[0]); err != nil {
			err = errors.Wrap(err, "parsing CA certificate")
			return
		}
	}

	if caCertFile != "" {
		var rootPEM []byte
		if rootPEM, err = ioutil.ReadFile(caCertFile); err != nil {
			err = errors.Wrap(err, "loading client certificate")
			return
		}
		tlsCfg := &tls.Config{
			RootCAs: x509.NewCertPool(),
		}
		if !tlsCfg.RootCAs.AppendCertsFromPEM(rootPEM) {
			err = errors.Wrap(err, "parsing client certificate")
			return
		}
		if clientCert.Leaf != nil {
			tlsCfg.Certificates = append(tlsCfg.Certificates, clientCert)
		}
		mongo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsCfg)
		}
	}
	mongo.Timeout = ConnectTimeout
	if mongoSession, err = mgo.DialWithInfo(mongo); err != nil {
		err = errors.Wrap(err, "connecting")
		return
	}
	mongoSession.SetMode(mgo.Monotonic, true)
	mongoSession.SetSafe(&mgo.Safe{WMode: "majority"})
	if clientCert.Leaf != nil {
		if err = mongoSession.Login(&mgo.Credential{
			Mechanism: "MONGODB-X509",
			Source:    "$external",
			Username:  getUsername(clientCert.Leaf),
		}); err != nil {
			err = errors.Wrap(err, "authenticating with x509 certificate")
			return
		}
	}
	db = &DB{mongoSession.DB(mongo.Database)}
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

// EnsureIndexes creates indices specified as a map from collection name to
// index list.
func (db *DB) EnsureIndexes(allIndexes map[string][]mgo.Index) error {
	for coll, idxs := range allIndexes {
		for _, idx := range idxs {
			if err := db.C(coll).EnsureIndex(idx); err != nil {
				return errors.Wrap(err, "creating MongoDB index")
			}
		}
	}
	return nil
}
