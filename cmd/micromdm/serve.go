package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/auth/basic"
	"github.com/go-kit/kit/log"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/groob/finalizer/logutil"
	"github.com/micromdm/go4/env"
	"github.com/micromdm/go4/httputil"
	"github.com/micromdm/go4/version"
	scep "github.com/vishnuvaradaraj/scep/server"
	"github.com/pkg/errors"
	"golang.org/x/crypto/acme/autocert"

	"github.com/vishnuvaradaraj/micromdm/mdm"
	"github.com/vishnuvaradaraj/micromdm/mdm/enroll"
	httputil2 "github.com/vishnuvaradaraj/micromdm/pkg/httputil"
	"github.com/vishnuvaradaraj/micromdm/platform/apns"
	"github.com/vishnuvaradaraj/micromdm/platform/appstore"
	appsbuiltin "github.com/vishnuvaradaraj/micromdm/platform/appstore/builtin"
	"github.com/vishnuvaradaraj/micromdm/platform/blueprint"
	blueprintbuiltin "github.com/vishnuvaradaraj/micromdm/platform/blueprint/builtin"
	"github.com/vishnuvaradaraj/micromdm/platform/command"
	"github.com/vishnuvaradaraj/micromdm/platform/config"
	depapi "github.com/vishnuvaradaraj/micromdm/platform/dep"
	"github.com/vishnuvaradaraj/micromdm/platform/dep/sync"
	"github.com/vishnuvaradaraj/micromdm/platform/device"
	devicebuiltin "github.com/vishnuvaradaraj/micromdm/platform/device/builtin"
	"github.com/vishnuvaradaraj/micromdm/platform/profile"
	block "github.com/vishnuvaradaraj/micromdm/platform/remove"
	"github.com/vishnuvaradaraj/micromdm/platform/user"
	userbuiltin "github.com/vishnuvaradaraj/micromdm/platform/user/builtin"
	"github.com/vishnuvaradaraj/micromdm/server"
)

const homePage = `<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>MicroMDM</title>
	<style>
		body {
			font-family: -apple-system, BlinkMacSystemFont, sans-serif;
		}
	</style>
</head>
<body>
	<h3>Welcome to MicroMDM!</h3>
	<p><a href="mdm/enroll">Enroll a device</a></p>
</body>
</html>
`

func serve(args []string) error {
	flagset := flag.NewFlagSet("serve", flag.ExitOnError)
	var (
		flConfigPath        = flagset.String("config-path", "/var/db/micromdm", "path to configuration directory")
		flServerURL         = flagset.String("server-url", "", "public HTTPS url of your server")
		flAPIKey            = flagset.String("api-key", env.String("MICROMDM_API_KEY", ""), "API Token for mdmctl command")
		flAPNSCertPath      = flagset.String("apns-cert", "", "path to APNS certificate")
		flAPNSKeyPass       = flagset.String("apns-password", env.String("MICROMDM_APNS_KEY_PASSWORD", ""), "password for your p12 APNS cert file (if using)")
		flAPNSKeyPath       = flagset.String("apns-key", "", "path to key file if using .pem push cert")
		flTLS               = flagset.Bool("tls", true, "use https")
		flTLSCert           = flagset.String("tls-cert", "", "path to TLS certificate")
		flTLSKey            = flagset.String("tls-key", "", "path to TLS private key")
		flHTTPAddr          = flagset.String("http-addr", ":https", "http(s) listen address of mdm server. defaults to :8080 if tls is false")
		flHTTPDebug         = flagset.Bool("http-debug", false, "enable debug for http(dumps full request)")
		flRepoPath          = flagset.String("filerepo", "", "path to http file repo")
		flDepSim            = flagset.String("depsim", "", "use depsim URL")
		flExamples          = flagset.Bool("examples", false, "prints some example usage")
		flCommandWebhookURL = flagset.String("command-webhook-url", "", "URL to send command responses.")
		flHomePage          = flagset.Bool("homepage", true, "hosts a simple built-in webpage at the / address")
	)
	flagset.Usage = usageFor(flagset, "micromdm serve [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	if *flExamples {
		printExamples()
		return nil
	}

	if *flServerURL == "" {
		return errors.New("must supply -server-url")
	}
	if !strings.HasPrefix(*flServerURL, "https://") {
		return errors.New("-server-url must begin with https://")
	}
	if !*flTLS && (*flTLSCert != "" || *flTLSKey != "") {
		return errors.New("cannot set -tls=false and supply -tls-cert or -tls-key")
	}
	if *flAPNSCertPath != "" || *flAPNSKeyPass != "" || *flAPNSKeyPath != "" {
		stdlog.Println("-apns-cert, -apns-password, and -apns-key switches are deprecated. please transition to `mdmctl mdmcert upload` instead")
	}

	logger := log.NewLogfmtLogger(os.Stderr)
	stdlog.SetOutput(log.NewStdlibAdapter(logger)) // force structured logs
	mainLogger := log.With(logger, "component", "main")
	mainLogger.Log("msg", "started")

	if err := os.MkdirAll(*flConfigPath, 0755); err != nil {
		return errors.Wrapf(err, "creating config directory %s", *flConfigPath)
	}
	sm := &server.Server{
		ConfigPath:          *flConfigPath,
		ServerPublicURL:     strings.TrimRight(*flServerURL, "/"),
		APNSCertificatePath: *flAPNSCertPath,
		APNSPrivateKeyPass:  *flAPNSKeyPass,
		APNSPrivateKeyPath:  *flAPNSKeyPath,
		Depsim:              *flDepSim,
		TLSCertPath:         *flTLSCert,
		CommandWebhookURL:   *flCommandWebhookURL,

		WebhooksHTTPClient: &http.Client{Timeout: time.Second * 30},

		// TODO: we have a static SCEP challenge password here to prevent
		// being prompted for the SCEP challenge which happens in a "normal"
		// (non-DEP) enrollment. While security is not improved it is at least
		// no less secure and prevents a useless dialog from showing.
		SCEPChallenge: "micromdm",
	}

	if err := sm.Setup(logger); err != nil {
		stdlog.Fatal(err)
	}
	syncer, err := sm.CreateDEPSyncer(logger)
	if err != nil {
		stdlog.Fatal(err)
	}

	var removeService block.Service
	{
		svc, err := block.New(sm.RemoveDB)
		if err != nil {
			stdlog.Fatal(err)
		}
		removeService = block.LoggingMiddleware(logger)(svc)
	}

	devDB, err := devicebuiltin.NewDB(sm.DB)
	if err != nil {
		stdlog.Fatal(err)
	}

	devWorker := device.NewWorker(devDB, sm.PubClient, logger)
	go devWorker.Run(context.Background())

	userDB, err := userbuiltin.NewDB(sm.DB)
	if err != nil {
		stdlog.Fatal(err)
	}
	userWorker := user.NewWorker(userDB, sm.PubClient, logger)
	go userWorker.Run(context.Background())

	bpDB, err := blueprintbuiltin.NewDB(sm.DB, sm.ProfileDB, userDB)
	if err != nil {
		stdlog.Fatal(err)
	}

	if err := bpDB.StartListener(sm.PubClient, sm.CommandService); err != nil {
		stdlog.Fatal(err)
	}

	ctx := context.Background()
	httpLogger := log.With(logger, "transport", "http")

	dc := sm.DEPClient
	appDB := &appsbuiltin.Repo{Path: *flRepoPath}

	scepEndpoints := scep.MakeServerEndpoints(sm.SCEPService)
	scepComponentLogger := log.With(logger, "component", "scep")
	scepEndpoints.GetEndpoint = scep.EndpointLoggingMiddleware(scepComponentLogger)(scepEndpoints.GetEndpoint)
	scepEndpoints.PostEndpoint = scep.EndpointLoggingMiddleware(scepComponentLogger)(scepEndpoints.PostEndpoint)
	scepHandler := scep.MakeHTTPHandler(scepEndpoints, sm.SCEPService, scepComponentLogger)

	enrollHandlers := enroll.MakeHTTPHandlers(ctx, enroll.MakeServerEndpoints(sm.EnrollService, sm.SCEPDepot), httptransport.ServerErrorLogger(httpLogger))

	r, options := httputil2.NewRouter(logger)

	r.Handle("/version", version.Handler())
	r.Handle("/mdm/enroll", enrollHandlers.EnrollHandler).Methods("GET", "POST")
	r.Handle("/ota/enroll", enrollHandlers.OTAEnrollHandler)
	r.Handle("/ota/phase23", enrollHandlers.OTAPhase2Phase3Handler).Methods("POST")
	r.Handle("/scep", scepHandler)
	if *flHomePage {
		r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, homePage)
		})
	}

	mdmEndpoints := mdm.MakeServerEndpoints(sm.MDMService)
	mdm.RegisterHTTPHandlers(r, mdmEndpoints, logger)

	// API commands. Only handled if the user provides an api key.
	if *flAPIKey != "" {
		basicAuthEndpointMiddleware := basic.AuthMiddleware("micromdm", *flAPIKey, "micromdm")

		configsvc := config.New(sm.ConfigDB)
		configEndpoints := config.MakeServerEndpoints(configsvc, basicAuthEndpointMiddleware)
		config.RegisterHTTPHandlers(r, configEndpoints, options...)

		apnsEndpoints := apns.MakeServerEndpoints(sm.APNSPushService, basicAuthEndpointMiddleware)
		apns.RegisterHTTPHandlers(r, apnsEndpoints, options...)

		devicesvc := device.New(devDB)
		deviceEndpoints := device.MakeServerEndpoints(devicesvc, basicAuthEndpointMiddleware)
		device.RegisterHTTPHandlers(r, deviceEndpoints, options...)

		profilesvc := profile.New(sm.ProfileDB)
		profileEndpoints := profile.MakeServerEndpoints(profilesvc, basicAuthEndpointMiddleware)
		profile.RegisterHTTPHandlers(r, profileEndpoints, options...)

		blueprintsvc := blueprint.New(bpDB)
		blueprintEndpoints := blueprint.MakeServerEndpoints(blueprintsvc, basicAuthEndpointMiddleware)
		blueprint.RegisterHTTPHandlers(r, blueprintEndpoints, options...)

		blockEndpoints := block.MakeServerEndpoints(removeService, basicAuthEndpointMiddleware)
		block.RegisterHTTPHandlers(r, blockEndpoints, options...)

		usersvc := user.New(userDB)
		userEndpoints := user.MakeServerEndpoints(usersvc, basicAuthEndpointMiddleware)
		user.RegisterHTTPHandlers(r, userEndpoints, options...)

		appsvc := appstore.New(appDB)
		appEndpoints := appstore.MakeServerEndpoints(appsvc, basicAuthEndpointMiddleware)
		appstore.RegisterHTTPHandlers(r, appEndpoints, options...)

		commandEndpoints := command.MakeServerEndpoints(sm.CommandService, basicAuthEndpointMiddleware)
		command.RegisterHTTPHandlers(r, commandEndpoints, options...)

		depsvc := depapi.New(dc, sm.PubClient)
		depEndpoints := depapi.MakeServerEndpoints(depsvc, basicAuthEndpointMiddleware)
		depapi.RegisterHTTPHandlers(r, depEndpoints, options...)

		depsyncEndpoints := sync.MakeServerEndpoints(sync.NewService(syncer, sm.SyncDB), basicAuthEndpointMiddleware)
		sync.RegisterHTTPHandlers(r, depsyncEndpoints, options...)
	} else {
		mainLogger.Log("msg", "no api key specified")
	}

	if *flRepoPath != "" {
		if _, err := os.Stat(*flRepoPath); os.IsNotExist(err) {
			stdlog.Fatal(err)
		}
		r.PathPrefix("/repo/").Handler(http.StripPrefix("/repo/", http.FileServer(http.Dir(*flRepoPath))))
	}

	var handler http.Handler
	if *flHTTPDebug {
		handler = httputil.HTTPDebugMiddleware(os.Stdout, true, logger.Log)(r)
	} else {
		handler = r
	}
	handler = logutil.NewHTTPLogger(httpLogger).Middleware(handler)

	srvURL, err := url.Parse(sm.ServerPublicURL)
	if err != nil {
		return errors.Wrapf(err, "parsing serverURL %q", sm.ServerPublicURL)
	}

	serveOpts := serveOptions(
		handler,
		*flHTTPAddr,
		srvURL.Hostname(),
		logger,
		*flTLSCert,
		*flTLSKey,
		sm.ConfigPath,
		*flTLS,
	)
	err = httputil.ListenAndServe(serveOpts...)
	return errors.Wrap(err, "calling ListenAndServe")
}

// serveOptions configures the []httputil.Options for ListenAndServe
func serveOptions(
	handler http.Handler,
	addr string,
	hostname string,
	logger log.Logger,
	certPath string,
	keyPath string,
	configPath string,
	tls bool,
) []httputil.Option {
	tlsFromFile := (certPath != "" && keyPath != "")
	serveOpts := []httputil.Option{
		httputil.WithACMEHosts([]string{hostname}),
		httputil.WithLogger(logger),
		httputil.WithHTTPHandler(handler),
	}
	if tlsFromFile {
		serveOpts = append(serveOpts, httputil.WithKeyPair(certPath, keyPath))
	}
	if !tls && addr == ":https" {
		serveOpts = append(serveOpts, httputil.WithAddress(":8080"))
	}
	if tls {
		serveOpts = append(serveOpts, httputil.WithAutocertCache(autocert.DirCache(filepath.Join(configPath, "le-certificates"))))
	}
	if addr != ":https" {
		serveOpts = append(serveOpts, httputil.WithAddress(addr))
	}
	return serveOpts
}

func printExamples() {
	const exampleText = `
		Quickstart:
		sudo micromdm serve -apns-cert /path/to/mdm_push_cert.p12 -apns-password=password_for_p12 -server-url=https://my-server-url

		Using self-signed certs:
		*Note, -apns flags are still required!*
		sudo micromdm serve -tls-cert=/path/to/server.crt -tls-key=/path/to/server.key

		`
	fmt.Println(exampleText)
}
