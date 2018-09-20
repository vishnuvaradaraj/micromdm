package builtin

import (
	"fmt"
	"context"

	"github.com/boltdb/bolt"
	"github.com/vishnuvaradaraj/micromdm/platform/remove"
	"github.com/pkg/errors"
	"cloud.google.com/go/firestore"
)

const RemoveBucket = "mdm.RemoveDevice"

type FireDB struct {
	*firestore.Client
}

func NewFireDB(db *firestore.Client) (*FireDB, error) {
	datastore := &FireDB{Client: db}
	return datastore, nil
}

func (db *FireDB) DeviceByUDID(udid string) (*remove.Device, error) {
	var dev remove.Device

	ctx := context.Background()

	doc, err := db.Collection(RemoveBucket).Doc(udid).Get(ctx)
	if err != nil {
		return nil, &notFound{"RemoveDevice","Not found"}
	}

	err = doc.DataTo(&dev)
	if err != nil {
		return nil, err
	}

	return &dev, nil
}

func (db *FireDB) Save(dev *remove.Device) error {

	ctx := context.Background()

	_, err := db.Collection(RemoveBucket).Doc(dev.UDID).Set(ctx, dev)
	if err != nil {
		return err
	}

	return nil
}

func (db *FireDB) Delete(udid string) error {

	ctx := context.Background()

	doc, err := db.Collection(RemoveBucket).Doc(udid).Get(ctx)
	if err != nil {
		return &notFound{"RemoveDevice","Not found"}
	}

	_, err = doc.Ref.Delete(ctx)
	return err
}

//////////////////////////////////////////////////////

type DB struct {
	*bolt.DB
}

func NewDB(db *bolt.DB) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(RemoveBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", RemoveBucket)
	}
	datastore := &DB{
		DB: db,
	}
	return datastore, nil
}

func (db *DB) DeviceByUDID(udid string) (*remove.Device, error) {
	var dev remove.Device
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(RemoveBucket))
		v := b.Get([]byte(udid))
		if v == nil {
			return &notFound{"Device", fmt.Sprintf("udid %s", udid)}
		}
		return remove.UnmarshalDevice(v, &dev)
	})
	return &dev, errors.Wrap(err, "remove: get device by udid")
}

func (db *DB) Save(dev *remove.Device) error {
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(RemoveBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", RemoveBucket)
	}
	pb, err := remove.MarshalDevice(dev)
	if err != nil {
		return errors.Wrap(err, "marshalling Device")
	}
	key := []byte(dev.UDID)
	if err := bkt.Put(key, pb); err != nil {
		return errors.Wrap(err, "put device to boltdb")
	}
	return tx.Commit()
}

func (db *DB) Delete(udid string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(RemoveBucket))
		v := b.Get([]byte(udid))
		if v == nil {
			return &notFound{"Device", fmt.Sprintf("udid %s", udid)}
		}
		return b.Delete([]byte(udid))
	})
	return errors.Wrapf(err, "delete device with udid %s", udid)
}

type notFound struct {
	ResourceType string
	Message      string
}

func (e *notFound) Error() string {
	return fmt.Sprintf("not found: %s %s", e.ResourceType, e.Message)
}

func (e *notFound) NotFound() bool {
	return true
}
