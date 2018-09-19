package builtin

import (
	"firebase.google.com/go"
	"google.golang.org/api/option"
	"io/ioutil"
	"os"
	"testing"

	"context"

	"github.com/boltdb/bolt"
	"github.com/vishnuvaradaraj/micromdm/platform/blueprint"
	profile "github.com/vishnuvaradaraj/micromdm/platform/profile/builtin"
	user "github.com/vishnuvaradaraj/micromdm/platform/user/builtin"
)

func TestSave(t *testing.T) {
	db := setupFireDB(t)
	bp := &blueprint.Blueprint{}
	bp.ApplyAt = []string{"Enroll"}

	if err := db.Save(bp); err == nil {
		t.Fatal("blueprints are required to have an UUID and Name")
	}

	bp.UUID = ""
	bp.Name = "blueprint"
	if err := db.Save(bp); err == nil {
		t.Fatal("blueprints are required to have an UUID")
	}

	bp.UUID = "a-b-c-d"
	bp.Name = ""
	if err := db.Save(bp); err == nil {
		t.Fatal("blueprints are required to have a Name")
	}

	bp.UUID = "a-b-c-d"
	bp.Name = "blueprint"
	if err := db.Save(bp); err != nil {
		t.Fatalf("saving blueprint in datastore: %s", err)
	}

	bp.UUID = "e-f-g-h"
	bp.Name = "blueprint"
	if err := db.Save(bp); err == nil {
		t.Fatal("blueprint names must be unique")
	}

	bp.UUID = "e-f-g-h"
	bp.Name = "blueprint2"
	if err := db.Save(bp); err != nil {
		t.Fatalf("saving blueprint2 in datastore: %s", err)
	}

	/*
	byName, err := db.BlueprintByName("blueprint")
	if err != nil {
		t.Fatalf("getting blueprint by Name: %s", err)
	}
	if byName == nil || byName.UUID != "a-b-c-d" {
		t.Fatalf("have %s, want %s", byName.UUID, "a-b-c-d")
	}
	*/
	byApplyAt, err := db.BlueprintsByApplyAt("Enroll")
	if err != nil {
		t.Fatalf("getting blueprint by ApplyAt: %s", err)
	}
	if len(byApplyAt) != 2 {
		t.Fatalf("multiple blueprints not saved correctly")
	}
}

func TestList(t *testing.T) {
	db := setupDB(t)
	bp1 := &blueprint.Blueprint{
		UUID: "a-b-c-d",
		Name: "blueprint-1",
	}
	bp2 := &blueprint.Blueprint{
		UUID: "e-f-g-h",
		Name: "blueprint-2",
	}

	if err := db.Save(bp1); err != nil {
		t.Fatalf("saving blueprint-1 to datastore: %s", err)
	}

	if err := db.Save(bp2); err != nil {
		t.Fatalf("saving blueprint-2 to datastore: %s", err)
	}

	bps, err := db.List()
	if err != nil {
		t.Fatalf("listing blueprints: %s", err)
	}
	if len(bps) != 2 {
		t.Fatalf("expected %d, found %d", 2, len(bps))
	}
}

func TestDelete(t *testing.T) {
	db := setupDB(t)
	bp1 := &blueprint.Blueprint{
		UUID: "a-b-c-d",
		Name: "blueprint",
	}

	if err := db.Save(bp1); err != nil {
		t.Fatalf("saving blueprint to datastore: %s", err)
	}

	if err := db.Delete("blueprint"); err != nil {
		t.Fatalf("deleting blueprint in datastore: %s", err)
	}

	_, err := db.BlueprintByName("blueprint")
	if err == nil {
		t.Fatalf("expected blueprint to be deleted: %s", err)
	}
}

func setupDB(t *testing.T) *DB {
	f, _ := ioutil.TempFile("", "bolt-")
	f.Close()
	os.Remove(f.Name())

	db, err := bolt.Open(f.Name(), 0777, nil)
	if err != nil {
		t.Fatalf("couldn't open bolt, err %s\n", err)
	}
	profileDB, err := profile.NewDB(db)
	if err != nil {
		t.Fatalf("couldn't create profile DB, err %s\n", err)
	}
	userDB, err := user.NewDB(db)
	if err != nil {
		t.Fatalf("couldn't create user DB, err %s\n", err)
	}
	blueprintDB, err := NewDB(db, profileDB, userDB)
	if err != nil {
		t.Fatalf("couldn't create blueprint DB, err %s\n", err)
	}
	return blueprintDB
}

func setupFireDB(t *testing.T) *FireDB {

	ctx := context.Background()

	// Use a service account
	sa := option.WithCredentialsFile("/Users/vishnuv/go/src/github.com/vishnuvaradaraj/micromdm/tools/certs/family-protection-dd191-firebase-adminsdk-x7xrc-435ea7bf3a.json")
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		t.Fatalf("couldn't open firebase, err %s\n", err)
	}

	fireDB, err := app.Firestore(ctx)
	if err != nil {
		t.Fatalf("couldn't init firebase, err %s\n", err)
	}

	profileDB, err := profile.NewFireDB(fireDB)
	if err != nil {
		t.Fatalf("couldn't create profile DB, err %s\n", err)
	}
	userDB, err := user.NewFireDB(fireDB)
	if err != nil {
		t.Fatalf("couldn't create user DB, err %s\n", err)
	}
	blueprintDB, err := NewFireDB(fireDB, profileDB, userDB)
	if err != nil {
		t.Fatalf("couldn't create blueprint DB, err %s\n", err)
	}
	return blueprintDB
}
