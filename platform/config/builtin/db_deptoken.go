package builtin

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/vishnuvaradaraj/micromdm/pkg/crypto"
	"github.com/vishnuvaradaraj/micromdm/platform/config"
)

const (
	depTokenBucket = "mdm.DEPToken"
)


func (db *FireDB) AddToken(consumerKey string, jsonData []byte) error {

	ctx := context.Background()

	var depToken config.DEPToken
	err := json.Unmarshal(jsonData, &depToken)
	if err != nil {
		return  err
	}

	_, err = db.Collection(depTokenBucket).Doc(consumerKey).Set(ctx, depToken)
	if err != nil {
		return err
	}

	if err := db.Publisher.Publish(context.TODO(), config.DEPTokenTopic, jsonData); err != nil {
		return err
	}

	return nil
}

func (db *FireDB) DEPTokens() ([]config.DEPToken, error) {
	var result []config.DEPToken

	ctx := context.Background()

	prefix := []byte("CK_")
	suffix := []byte("CK`")

	q := db.Collection(depTokenBucket).Where("ConsumerKey", ">=", prefix).Where("ConsumerKey", "<", suffix)
	query := q.Documents(ctx)

	docs, err := query.GetAll()
	if err != nil {
		return nil, err
	}

	if (len(docs)>0) {

		for i := 0; i < len(docs); i++ {
			var depToken config.DEPToken

			doc := docs[i]
			doc.DataTo(&depToken)

			result = append(result, depToken)
		}
	}

	return result, err
}

func (db *FireDB) DEPKeypair() (key *rsa.PrivateKey, cert *x509.Certificate, err error) {
	var keyBytes, certBytes []byte

	ctx := context.Background()

	depKeyPair, err := db.Collection(depTokenBucket).Doc("key").Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	depMap := depKeyPair.Data()

	keyBytes = depMap["key"].([]byte)
	certBytes = depMap["value"].([]byte)

	if keyBytes == nil || certBytes == nil {
		// if there is no certificate or private key then generate
		key, cert, err = generateAndStoreDEPKeypairFire(db)
	} else {
		key, err = x509.ParsePKCS1PrivateKey(keyBytes)
		if err != nil {
			return
		}
		cert, err = x509.ParseCertificate(certBytes)
		if err != nil {
			return
		}
	}
	return
}

func generateAndStoreDEPKeypairFire(db *FireDB) (key *rsa.PrivateKey, cert *x509.Certificate, err error) {
	key, cert, err = crypto.SimpleSelfSignedRSAKeypair("micromdm-dep-token", 365)
	if err != nil {
		return
	}

	pkBytes := x509.MarshalPKCS1PrivateKey(key)
	certBytes := cert.Raw

	ctx := context.Background()
	_, err = db.Collection(depTokenBucket).Doc("key").Set(ctx, map[string]interface{}{
		"key":    pkBytes,
		"certificate":   certBytes,
	})

	return
}

//////////////////////////////////////////////////////

func (db *DB) AddToken(consumerKey string, json []byte) error {
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(depTokenBucket))
		if err != nil {
			return err
		}
		return b.Put([]byte(consumerKey), json)
	})
	if err != nil {
		return err
	}
	if err := db.Publisher.Publish(context.TODO(), config.DEPTokenTopic, json); err != nil {
		return err
	}
	return nil
}

func (db *DB) DEPTokens() ([]config.DEPToken, error) {
	var result []config.DEPToken
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(depTokenBucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()

		prefix := []byte("CK_")
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var depToken config.DEPToken
			err := json.Unmarshal(v, &depToken)
			if err != nil {
				// TODO: log problematic DEP token, or remove altogether?
				continue
			}
			result = append(result, depToken)
		}
		return nil
	})
	return result, err
}

func (db *DB) DEPKeypair() (key *rsa.PrivateKey, cert *x509.Certificate, err error) {
	var keyBytes, certBytes []byte
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(depTokenBucket))
		if b == nil {
			return nil
		}
		keyBytes = b.Get([]byte("key"))
		certBytes = b.Get([]byte("certificate"))
		return nil
	})
	if err != nil {
		return
	}
	if keyBytes == nil || certBytes == nil {
		// if there is no certificate or private key then generate
		key, cert, err = generateAndStoreDEPKeypair(db)
	} else {
		key, err = x509.ParsePKCS1PrivateKey(keyBytes)
		if err != nil {
			return
		}
		cert, err = x509.ParseCertificate(certBytes)
		if err != nil {
			return
		}
	}
	return
}

func generateAndStoreDEPKeypair(db *DB) (key *rsa.PrivateKey, cert *x509.Certificate, err error) {
	key, cert, err = crypto.SimpleSelfSignedRSAKeypair("micromdm-dep-token", 365)
	if err != nil {
		return
	}

	pkBytes := x509.MarshalPKCS1PrivateKey(key)
	certBytes := cert.Raw

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(depTokenBucket))
		if err != nil {
			return err
		}
		err = b.Put([]byte("key"), pkBytes)
		if err != nil {
			return err
		}
		err = b.Put([]byte("certificate"), certBytes)
		if err != nil {
			return err
		}
		return nil
	})

	return
}
