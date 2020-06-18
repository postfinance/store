package etcd_test

import (
	"log"

	"git.pnet.ch/golang/pkg/store/etcd"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ExampleWithDialOptions() {
	e, err := etcd.New(etcd.WithDialOptions(grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 8))))
	if err != nil {
		log.Fatal(err)
	}

	_, _ = e.Get("/key")
}

func ExampleWithErrorHandler() {
	handleError := func(err error) error {
		fatal := func(err error, msg string) {
			panic(errors.Wrap(err, msg))
		}
		// handle grpc error if code is available
		// https://github.com/grpc/grpc-go/blob/master/codes/codes.go
		switch status.Code(err) {
		case codes.Unauthenticated:
			fatal(err, "grpc Unauthenticated error - terminating")
		case codes.ResourceExhausted:
			fatal(err, "grpc ResourceExhausted error - terminating")
		case codes.Internal:
			fatal(err, "grpc Internal error - terminating")
		case codes.Unavailable:
			fatal(err, "grpc Unavailable error - terminating")
		}
		// handle etcd server-side errors
		// https://github.com/coreos/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
		switch err {
		case rpctypes.ErrNoSpace,
			rpctypes.ErrTooManyRequests: // codes.ResourceExhausted
			fatal(err, "terminating")
		case rpctypes.ErrInvalidAuthToken: // codes.Unauthenticated
			fatal(err, "terminating")
		case rpctypes.ErrNoLeader,
			rpctypes.ErrNotCapable,
			rpctypes.ErrStopped,
			rpctypes.ErrTimeout,
			rpctypes.ErrTimeoutDueToLeaderFail,
			rpctypes.ErrTimeoutDueToConnectionLost,
			rpctypes.ErrUnhealthy: // codes.Unavailable
			fatal(err, "terminating")
		}

		return err
	}

	e, err := etcd.New(etcd.WithErrorHandler(handleError))
	if err != nil {
		log.Fatal(err)
	}

	_, _ = e.Get("/key")
}
