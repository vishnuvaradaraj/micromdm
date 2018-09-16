// Package command provides utilities for creating MDM Payloads.
package command

import (
	"github.com/vishnuvaradaraj/micromdm/mdm/mdm"
	"github.com/vishnuvaradaraj/micromdm/platform/pubsub"
	"golang.org/x/net/context"
)

type Service interface {
	NewCommand(context.Context, *mdm.CommandRequest) (*mdm.CommandPayload, error)
}

type CommandService struct {
	publisher pubsub.Publisher
}

func New(pub pubsub.Publisher) (*CommandService, error) {
	svc := CommandService{
		publisher: pub,
	}
	return &svc, nil
}
