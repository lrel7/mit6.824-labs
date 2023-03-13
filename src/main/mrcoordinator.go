package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
)

func main() {
	// check args
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	// init the coordinator
	m := mr.MakeCoordinator(os.Args[1:], 10)
	// wait until it finishes
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
