package public

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckFilePath(t *testing.T) {
	fileName := "./data/tuvssh_0"
	err := CheckCreatePath(fileName)
	if err != nil {
		t.Errorf(err.Error())
	} else {
		t.Logf(filepath.Dir(fileName))
		_ = os.Remove(filepath.Dir(fileName))
	}
}
