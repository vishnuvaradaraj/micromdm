package builtin

import (
	"encoding/json"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/firestore"
	"context"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"

	"github.com/vishnuvaradaraj/micromdm/platform/dep/sync"
)

const (
	ConfigBucket     = "mdm.DEPConfig"
	AutoAssignBucket = "mdm.DEPAutoAssign"
)

type FireDB struct {
	*firestore.Client
}

func NewFireDB(db *firestore.Client) (*FireDB, error) {
	datastore := &FireDB{Client: db}
	return datastore, nil
}

func (db *FireDB) LoadCursor() (*sync.Cursor, error) {

	var cur sync.Cursor

	ctx := context.Background()

	doc, err := db.Collection(ConfigBucket).Doc("configuration").Get(ctx)
	if err != nil {
		return nil, err
	}

	err = doc.DataTo(&cur)
	if err != nil {
		return nil, err
	}

	return &cur, nil
}

func (db *FireDB) SaveCursor(c sync.Cursor) error {

	ctx := context.Background()

	_, err := db.Collection(ConfigBucket).Doc("configuration").Set(ctx, c)
	if err != nil {
		return err
	}

	return nil
}

func (db *FireDB) SaveAutoAssigner(a *sync.AutoAssigner) error {
	if a.Filter != "*" {
		return errors.New("only '*' filter auto-assigners supported")
	}
	ctx := context.Background()

	_, err := db.Collection(AutoAssignBucket).Doc(a.Filter).Set(ctx, a)
	if err != nil {
		return err
	}

	return nil
}

func (db *FireDB) DeleteAutoAssigner(filter string) error {

	ctx := context.Background()

	_, err := db.Collection(AutoAssignBucket).Doc(filter).Delete(ctx)
	return err
}

func (db *FireDB) LoadAutoAssigners() ([]sync.AutoAssigner, error) {
	var aal []sync.AutoAssigner
	ctx := context.Background()

	iter := db.Collection(AutoAssignBucket).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var aa sync.AutoAssigner
		doc.DataTo(&aa)

		aal = append(aal, aa)
	}

	return aal, nil
}


//////////////////////////////////////////////////////

type DB struct {
	*bolt.DB
}

func NewDB(db *bolt.DB) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(ConfigBucket))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(AutoAssignBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", ConfigBucket)
	}
	datastore := &DB{DB: db}
	return datastore, nil
}

func (db *DB) LoadCursor() (*sync.Cursor, error) {
	var cursor = struct {
		Cursor sync.Cursor `json:"cursor"`
	}{}
	err := db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(ConfigBucket))
		v := bkt.Get([]byte("configuration"))
		if v == nil {
			return nil // TODO add notfound
		}
		err := json.Unmarshal(v, &cursor)
		return errors.Wrap(err, "unmarshal dep cursor")
	})
	if err != nil {
		return nil, errors.Wrap(err, "load cursor from bolt")
	}
	return &cursor.Cursor, nil
}

func (db *DB) SaveCursor(c sync.Cursor) error {
	err := db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(ConfigBucket))
		if err != nil {
			return err
		}
		// use anonymous struct to preserve old structure.
		var cursor = struct {
			Cursor sync.Cursor `json:"cursor"`
		}{Cursor: c}
		v, err := json.Marshal(&cursor)
		if err != nil {
			return err
		}
		return bkt.Put([]byte("configuration"), v)
	})
	return errors.Wrap(err, "saving dep sync cursor")
}

func (db *DB) SaveAutoAssigner(a *sync.AutoAssigner) error {
	if a.Filter != "*" {
		return errors.New("only '*' filter auto-assigners supported")
	}
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(AutoAssignBucket))
		if err != nil {
			return err
		}
		return b.Put([]byte(a.Filter), []byte(a.ProfileUUID))
	})
	return errors.Wrap(err, "saving auto-assigner")
}

func (db *DB) DeleteAutoAssigner(filter string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(AutoAssignBucket))
		if b == nil { // bucket doesn't exist yet
			return nil
		}
		return b.Delete([]byte(filter))
	})
}

func (db *DB) LoadAutoAssigners() ([]sync.AutoAssigner, error) {
	var aa []sync.AutoAssigner
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(AutoAssignBucket))
		if b == nil { // bucket doesn't exist yet
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			aa = append(aa, sync.AutoAssigner{
				Filter:      string(k),
				ProfileUUID: string(v),
			})
			return nil
		})
	})
	return aa, errors.Wrap(err, "loading auto-assigners")
}
