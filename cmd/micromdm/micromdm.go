package main

import (
	"flag"
	"fmt"
	"google.golang.org/api/iterator"
	"math/rand"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/net/context"

	firebase "firebase.google.com/go"
	"google.golang.org/api/option"
	"github.com/micromdm/go4/version"
	"log"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {

	ctx := context.Background()

	// Use a service account
	sa := option.WithCredentialsFile("/Users/vishnuv/go/src/github.com/vishnuvaradaraj/micromdm/tools/certs/family-protection-dd191-firebase-adminsdk-x7xrc-435ea7bf3a.json")
	app, err := firebase.NewApp(ctx, nil, sa)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Close()

	_, _, err = client.Collection("Devices").Add(ctx, map[string]interface{}{
		"first": "Ada",
		"last":  "Lovelace",
		"born":  1815,
	})
	if err != nil {
		log.Fatalf("Failed adding alovelace: %v", err)
	}

	iter := client.Collection("users").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate: %v", err)
		}
		fmt.Println(doc.Data())
	}

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var run func([]string) error
	switch strings.ToLower(os.Args[1]) {
	case "version", "-version":
		version.Print()
		return
	case "serve":
		run = serve
	default:
		usage()
		os.Exit(1)
	}

	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func usage() error {
	helpText := `USAGE: micromdm <COMMAND>

Available Commands:
	serve
	version

Use micromdm <command> -h for additional usage of each command.
Example: micromdm serve -h
`
	fmt.Println(helpText)
	return nil
}

func usageFor(fs *flag.FlagSet, short string) func() {
	return func() {
		fmt.Fprintf(os.Stderr, "USAGE\n")
		fmt.Fprintf(os.Stderr, "  %s\n", short)
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "FLAGS\n")
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "\t-%s %s\t%s\n", f.Name, f.DefValue, f.Usage)
		})
		w.Flush()
		fmt.Fprintf(os.Stderr, "\n")
	}
}
