package sybil

import "flag"
import "log"
import "fmt"
import "os"

// extracted from and influenced by
// https://groups.google.com/forum/#!topic/golang-nuts/ct99dtK2Jo4
var DEBUG_FLAG *bool = flag.Bool("debug", false, "enable debug logging")

// use env variable DEBUG=1 to turn on debug output
var ENV_FLAG = os.Getenv("DEBUG")

func Print(args ...interface{}) {
	fmt.Println(args...)
}

func Warn(args ...interface{}) {
	fmt.Println(append([]interface{}{"Warning:"}, args...)...)
}

func Debug(args ...interface{}) {
	if *DEBUG_FLAG || ENV_FLAG != "" {
		log.Println(args...)
	}
}

func Error(args ...interface{}) {
	log.Fatalln(append([]interface{}{"ERROR"}, args...)...)
}
