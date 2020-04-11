package api

import "os/exec"

// This API will calls digest on a sybil table
func (t *SybilTable) DigestRecords() {

	flags := []string{"digest", "-table", t.Config.Table, "-dir", t.Config.Dir}
	cmd := exec.Command(SYBIL_BIN, flags...)

	Debug("DIGESTING TABLE ", t.Config.Table)

	out, err := cmd.Output()
	if err != nil {
		Error("CAN'T DIGEST TABLE", out, err)
	} else {
		Debug("SUCCESSFULLY DIGESTED RECORDS IN ", t.Config.Table)
		t.NewRecords = make([]interface{}, 0)
	}

}

// }}} INGESTION
