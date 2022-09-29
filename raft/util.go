package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)

	}
	return
}

var debugStart time.Time
var debugVerbosity int

func Microseconds(t time.Time) string {
	return fmt.Sprintf("%06d", t.Sub(debugStart).Microseconds()/100)
}
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}
