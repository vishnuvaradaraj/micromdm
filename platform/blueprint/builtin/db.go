package builtin

import (
	"fmt"
	"google.golang.org/api/iterator"
	"strings"
	"context"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"

	"cloud.google.com/go/firestore"

	"github.com/vishnuvaradaraj/micromdm/platform/blueprint"
	"github.com/vishnuvaradaraj/micromdm/platform/profile"
	"github.com/vishnuvaradaraj/micromdm/platform/user"
)

const (
	BlueprintBucket      = "mdm.Blueprint"
	blueprintIndexBucket = "mdm.BlueprintIdx"
)

type FireDB struct {
	*firestore.Client
	profDB profile.Store
	userDB user.Store
}

func NewFireDB(
	db *firestore.Client,
	profileDB profile.Store,
	userDB user.Store,) (*FireDB, error) {

	datastore := &FireDB{
		Client: db,
		profDB: profileDB,
		userDB: userDB,}
	return datastore, nil
}

func (db *FireDB) List() ([]blueprint.Blueprint, error) {
	// TODO add filter/limit with ForEach
	var blueprints []blueprint.Blueprint

	ctx := context.Background()

	iter := db.Collection(BlueprintBucket).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var bp blueprint.Blueprint
		doc.DataTo(&bp)

		blueprints = append(blueprints, bp)
	}

	return blueprints, nil
}

func (db *FireDB) Save(bp *blueprint.Blueprint) error {

	err := bp.Verify()
	if err != nil {
		return err
	}
	check_bp, err := db.BlueprintByName(bp.Name)
	if err != nil && !isNotFound(err) {
		return err
	}
	if err == nil && bp.UUID != check_bp.UUID {
		return fmt.Errorf("Blueprint not saved: same name %s exists", bp.Name)
	}
	// verify that each Profile ID represents a profile we know about
	for _, p := range bp.ProfileIdentifiers {
		if _, err := db.profDB.ProfileById(p); err != nil {
			if profile.IsNotFound(err) {
				return fmt.Errorf("Profile ID %s in Blueprint %s does not exist", p, bp.Name)
			}
			return errors.Wrap(err, "fetching profile")
		}
	}

	ctx := context.Background()

	_, err = db.Collection(BlueprintBucket).Doc(bp.UUID).Set(ctx, bp)
	if err != nil {
		return err
	}

	return nil
}

func (db *FireDB) BlueprintByName(name string) (*blueprint.Blueprint, error) {
	var bp blueprint.Blueprint

	ctx := context.Background()

	q := db.Collection(BlueprintBucket).Where("Name", "==", name).Limit(1)
	query := q.Documents(ctx)
	docs, err := query.GetAll()
	if err != nil {
		return nil, err
	}

	if (len(docs)>0) {

		var bp blueprint.Blueprint

		doc := docs[0]
		doc.DataTo(&bp)

	} else {
		return nil, &notFound{"Blueprint","Not found"}
	}

	return &bp, nil
}

func (db *FireDB) BlueprintsByApplyAt(name string) ([]*blueprint.Blueprint, error) {

	var blueprints []*blueprint.Blueprint

	ctx := context.Background()

	iter := db.Collection(BlueprintBucket).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var bp blueprint.Blueprint
		doc.DataTo(&bp)

		for _, n := range bp.ApplyAt {
			if strings.ToLower(n) == strings.ToLower(name) {
				blueprints = append(blueprints, &bp)
				break
			}
		}
	}

	return blueprints, nil
}

func (db *FireDB) Delete(name string) error {

	ctx := context.Background()

	q := db.Collection(BlueprintBucket).Where("Name", "==", name).Limit(1)
	query := q.Documents(ctx)
	docs, err := query.GetAll()
	if err != nil {
		return err
	}

	if (len(docs)>0) {

		doc := docs[0]
		doc.Ref.Delete(ctx)

	} else {
		return &notFound{"Blueprint","Not found"}
	}
	return err
}

//////////////////////////////////////////////////////////////////

type DB struct {
	*bolt.DB
	profDB profile.Store
	userDB user.Store
}

func NewDB(
	db *bolt.DB,
	profileDB profile.Store,
	userDB user.Store,
) (*DB, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(blueprintIndexBucket))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(BlueprintBucket))
		return err
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating %s bucket", BlueprintBucket)
	}
	datastore := &DB{
		DB:     db,
		profDB: profileDB,
		userDB: userDB,
	}
	return datastore, nil
}

func (db *DB) List() ([]blueprint.Blueprint, error) {
	// TODO add filter/limit with ForEach
	var blueprints []blueprint.Blueprint
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BlueprintBucket))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var bp blueprint.Blueprint
			if err := blueprint.UnmarshalBlueprint(v, &bp); err != nil {
				return err
			}
			blueprints = append(blueprints, bp)
		}
		return nil
	})
	return blueprints, err
}

func (db *DB) Save(bp *blueprint.Blueprint) error {
	err := bp.Verify()
	if err != nil {
		return err
	}
	check_bp, err := db.BlueprintByName(bp.Name)
	if err != nil && !isNotFound(err) {
		return err
	}
	if err == nil && bp.UUID != check_bp.UUID {
		return fmt.Errorf("Blueprint not saved: same name %s exists", bp.Name)
	}
	// verify that each Profile ID represents a profile we know about
	for _, p := range bp.ProfileIdentifiers {
		if _, err := db.profDB.ProfileById(p); err != nil {
			if profile.IsNotFound(err) {
				return fmt.Errorf("Profile ID %s in Blueprint %s does not exist", p, bp.Name)
			}
			return errors.Wrap(err, "fetching profile")
		}
	}
	tx, err := db.DB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	bkt := tx.Bucket([]byte(BlueprintBucket))
	if bkt == nil {
		return fmt.Errorf("bucket %q not found!", BlueprintBucket)
	}
	bpproto, err := blueprint.MarshalBlueprint(bp)
	if err != nil {
		return errors.Wrap(err, "marshalling blueprint")
	}
	idxBucket := tx.Bucket([]byte(blueprintIndexBucket))
	if idxBucket == nil {
		return fmt.Errorf("bucket %v not found!", idxBucket)
	}
	key := []byte(bp.Name)
	if err := idxBucket.Put(key, []byte(bp.UUID)); err != nil {
		return errors.Wrap(err, "put blueprint idx to boltdb")
	}

	key = []byte(bp.UUID)
	if err := bkt.Put(key, bpproto); err != nil {
		return errors.Wrap(err, "put blueprint to boltdb")
	}
	return tx.Commit()
}

func (db *DB) BlueprintByName(name string) (*blueprint.Blueprint, error) {
	var bp blueprint.Blueprint
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BlueprintBucket))
		ib := tx.Bucket([]byte(blueprintIndexBucket))
		idx := ib.Get([]byte(name))
		if idx == nil {
			return &notFound{"Blueprint", fmt.Sprintf("name %s", name)}
		}
		v := b.Get(idx)
		if idx == nil {
			return &notFound{"Blueprint", fmt.Sprintf("uuid %s", string(idx))}
		}
		return blueprint.UnmarshalBlueprint(v, &bp)
	})
	if err != nil {
		return nil, err
	}
	return &bp, nil
}

func (db *DB) BlueprintsByApplyAt(name string) ([]*blueprint.Blueprint, error) {
	var bps []*blueprint.Blueprint
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BlueprintBucket))
		c := b.Cursor()
		// TODO: fix this to use an index of ApplyAt strings mapping to
		// an array of Blueprints or other more efficient means. Looping
		// over every blueprint is quite inefficient!
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var bp blueprint.Blueprint
			err := blueprint.UnmarshalBlueprint(v, &bp)
			if err != nil {
				fmt.Println("could not Unmarshal Blueprint")
				continue
			}
			for _, n := range bp.ApplyAt {
				if strings.ToLower(n) == strings.ToLower(name) {
					bps = append(bps, &bp)
					break
				}
			}
		}
		return nil
	})
	return bps, err
}

func (db *DB) Delete(name string) error {
	bp, err := db.BlueprintByName(name)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		// TODO: reformulate into a transaction?
		b := tx.Bucket([]byte(BlueprintBucket))
		i := tx.Bucket([]byte(blueprintIndexBucket))
		err := i.Delete([]byte(bp.Name))
		if err != nil {
			return err
		}
		err = b.Delete([]byte(bp.UUID))
		if err != nil {
			return err
		}
		return nil
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

func isNotFound(err error) bool {
	if _, ok := err.(*notFound); ok {
		return true
	}
	return false
}
