package api

import "os/exec"
import "encoding/json"
import sybil "github.com/logv/sybil/src/lib"

// {{{ TABLE INFO

func (t *SybilTable) GetTableInfo() *sybil.TableInfo {
	flags := []string{"query", "-table", t.Config.Table, "-dir", t.Config.Dir, "-info", "--read-log", "--json"}
	cmd := exec.Command(SYBIL_BIN, flags...)

	out, err := cmd.Output()
	Debug("OUTPUT", string(out))
	if err != nil {
		Error("CAN'T READ TABLE INFO FOR", t.Config.Table)
	} else {
		var unmarshalled sybil.TableInfo
		err := json.Unmarshal(out, &unmarshalled)
		if err != nil {
			Print("COULDN'T READ TABLE INFO FOR", t.Config.Table, "ERR", err)
		} else {
			return &unmarshalled
		}
	}

	return nil

}

// }}}
