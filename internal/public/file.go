package public

import (
	"os"
	"path/filepath"
)

func CheckCreatePath(fileName string) error {
	filePath := filepath.Dir(fileName)
	err := os.MkdirAll(filePath, 0655)
	return err
}
