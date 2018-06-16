// +build gofuzz

package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/logv/sybil/src/sybil"
)

func FuzzQuery(data []byte) int {
	print := os.Getenv("FUZZDEBUG") == "1"
	if print {
		fmt.Printf("+%q\n", string(data))
		defer fmt.Printf("-%q\n", string(data))
	}
	flags := sybil.FLAGS
	flags.TABLE = "a"
	if err := json.Unmarshal(data, &flags); err != nil {
		if print {
			fmt.Println("err:", err)
		}
		return 0
	}
	if print {
		fmt.Println(&flags)
	}
	if err := runQueryCmdLine(&flags); err != nil {
		if print {
			fmt.Println("err:", err)
		}
	}
	return 1
}
