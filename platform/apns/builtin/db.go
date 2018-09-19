package builtin

import (
	"fmt"
	"context"
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"

	"cloud.google.com/go/firestore"
	"github.com/vishnuvaradaraj/micromdm/platform/apns"
	"github.com/vishnuvaradaraj/micromdm/platform/pubsub"
)

const PushBucket = "mdm.PushInfo"

type FireDB struct {
	*firestore.Client
}

func NewFireDB(db *firestore.Client) (*FireDB, error) {
	datastore := &FireDB{Client: db}
	return datastore, nil
}

func (db *FireDB) PushInfo(udid string) (*apns.PushInfo, error) {
	var info apns.PushInfo

	ctx := context.Background()

	doc, err := db.Collection(PushBucket).Doc(udid).Get(ctx)
	if err != nil {
		return nil, &notFound{"Pushinfo","Not found"}
	}

	err = doc.DataTo(&info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (db *FireDB) Save(info *apns.PushInfo) error {

	ctx := context.Background()

	_, err := db.Collection(PushBucket).Doc(info.UDID).Set(ctx, info)
	if err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////

type DB struct {
	*bolt.DB
}

func NewDB(db *bolt.DB, sub pubsub.Subscriber) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(PushBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", PushBucket)
	}
	datastore := &DB{
		DB: db,
	}
	return datastore, nil
}

type notFound struct {
	ResourceType string
	Message      string
}

func (e *notFound) Error() string {
	return fmt.Sprintf("not found: %s %s", e.ResourceType, e.Message)
}

func (db *DB) PushInfo(udid string) (*apns.PushInfo, error) {
	var info apns.PushInfo
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(PushBucket))
		v := b.Get([]byte(udid))
		if v == nil {
			return &notFound{"PushInfo", fmt.Sprintf("udid %s", udid)}
		}
		return apns.UnmarshalPushInfo(v, &info)
	})
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (db *DB) Save(info *apns.PushInfo) error {
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(PushBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", PushBucket)
	}
	pushproto, err := apns.MarshalPushInfo(info)
	if err != nil {
		return errors.Wrap(err, "marshalling PushInfo")
	}
	key := []byte(info.UDID)
	if err := bkt.Put(key, pushproto); err != nil {
		return errors.Wrap(err, "put PushInfo to boltdb")
	}
	return tx.Commit()
}
