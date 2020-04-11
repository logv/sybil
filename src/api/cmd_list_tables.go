package api

import "os/exec"
import "encoding/json"

// {{{ TABLE INFO
func ListTables(config *SybilConfig) []string {
	flags := []string{"query", "-tables", "-dir", config.Dir, "--json"}
	cmd := exec.Command(SYBIL_BIN, flags...)

	out, err := cmd.Output()
	if err != nil {
		Error("CAN'T READ DB INFO FOR", config.Dir)
	} else {
		var unmarshalled []string
		err := json.Unmarshal(out, &unmarshalled)
		if err != nil {
			Print("COULDN'T READ TABLE LIST FOR", config.Table, "ERR", err)
		} else {
			return unmarshalled
		}
	}

	return []string{}

}

// }}}
