package public

import (
	"os"
	"path/filepath"
)

func CheckCreatePath(fileName string) error {
	filePath := filepath.Dir(fileName)
	err := os.MkdirAll(filePath, os.ModePerm)
	return err
}
