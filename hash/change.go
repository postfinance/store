package hash

import "github.com/postfinance/store"

// Constants
const (
	put change = iota
	del
)

// change represents the kind of change in the store
type change int

func (c change) String() string {
	switch c {
	case put:
		return "PUT"
	case del:
		return "DELETE"
	default:
		return "UNDEFINED"
	}
}

// notifyFunc will be called on every PUT or DELETE
type notifyCallbackFunc func(changeNotification)

// changeNotification will be sent to the watch channel if an entry was changed
type changeNotification struct {
	Type     change
	Data     store.Entry
	isCreate bool
}
