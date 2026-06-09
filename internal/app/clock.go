package app

import "time"

// RealClock implements usecase.Clock using the system clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }
