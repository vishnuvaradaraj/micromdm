package builtin

import (
	"fmt"
	"context"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/firestore"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/vishnuvaradaraj/micromdm/platform/profile"
)

const (
	ProfileBucket = "mdm.Profile"
)

type FireDB struct {
	*firestore.Client
}

func NewFireDB(db *firestore.Client) (*FireDB, error) {
	datastore := &FireDB{Client: db}
	return datastore, nil
}

func (db *FireDB) List() ([]profile.Profile, error) {

	var list []profile.Profile

	ctx := context.Background()

	iter := db.Collection(ProfileBucket).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var p profile.Profile
		doc.DataTo(&p)

		list = append(list, p)
	}

	return list, nil
}

func (db *FireDB) Save(p *profile.Profile) error {
	err := p.Validate()
	if err != nil {
		return err
	}

	ctx := context.Background()

	_, err = db.Collection(ProfileBucket).Doc(p.Identifier).Set(ctx, p)
	if err != nil {
		return err
	}

	return nil
}

func (db *FireDB) ProfileById(id string) (*profile.Profile, error) {
	var p profile.Profile

	ctx := context.Background()

	doc, err := db.Collection(ProfileBucket).Doc(id).Get(ctx)
	if err != nil {
		return nil, &notFound{"Profile","Not found"}
	}

	err = doc.DataTo(&p)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

func (db *FireDB) Delete(id string) error {

	ctx := context.Background()

	doc, err := db.Collection(ProfileBucket).Doc(id).Get(ctx)
	if err != nil {
		return &notFound{"Profile","Not found"}
	}

	_, err = doc.Ref.Delete(ctx)
	return err
}

//////////////////////////////////////////////////////////////////////////

type DB struct {
	*bolt.DB
}

func NewDB(db *bolt.DB) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(ProfileBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", ProfileBucket)
	}
	datastore := &DB{
		DB: db,
	}
	return datastore, nil
}

func (db *DB) List() ([]profile.Profile, error) {
	// TODO add filter/limit with ForEach
	var list []profile.Profile
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProfileBucket))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var p profile.Profile
			if err := profile.UnmarshalProfile(v, &p); err != nil {
				return err
			}
			list = append(list, p)
		}
		return nil
	})
	return list, err
}

func (db *DB) Save(p *profile.Profile) error {
	err := p.Validate()
	if err != nil {
		return err
	}
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(ProfileBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", ProfileBucket)
	}
	pproto, err := profile.MarshalProfile(p)
	if err != nil {
		return errors.Wrap(err, "marshalling profile")
	}
	if err := bkt.Put([]byte(p.Identifier), pproto); err != nil {
		return errors.Wrap(err, "put profile to boltdb")
	}
	return tx.Commit()
}

func (db *DB) ProfileById(id string) (*profile.Profile, error) {
	var p profile.Profile
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProfileBucket))
		v := b.Get([]byte(id))
		if v == nil {
			return &notFound{"Profile", fmt.Sprintf("id %s", id)}
		}
		return profile.UnmarshalProfile(v, &p)
	})
	return &p, err
}

func (db *DB) Delete(id string) error {
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProfileBucket))
		v := b.Get([]byte(id))
		if v == nil {
			return &notFound{"Profile", fmt.Sprintf("id %s", id)}
		}
		return b.Delete([]byte(id))
	})
	return err
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
