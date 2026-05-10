package domain

import "errors"

var ErrDuplicateEvent = errors.New("duplicate event")
var ErrStaleEvent = errors.New("stale event")
