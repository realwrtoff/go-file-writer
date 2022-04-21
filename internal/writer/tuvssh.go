package writer

import (
	"github.com/sirupsen/logrus"
)

type TuvsshWriter struct {
	*FileWriter
}

func NewTuvsshWriter(
	filePath string,
	runLog *logrus.Logger,
) *TuvsshWriter {
	return &TuvsshWriter{
		FileWriter: NewFileWriter(filePath, runLog),
	}
}

func (w *TuvsshWriter)WriteLine(fileName string, array []string) (err error) {
	for _, s := range array {
		_, _ = w.wfps[fileName].WriteString(s)
		_, _ = w.wfps[fileName].WriteString(",")
	}
	_, err = w.wfps[fileName].WriteString("\n")
	return
}
