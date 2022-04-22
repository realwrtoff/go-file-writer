package writer

import (
	"bufio"
	"github.com/hatlonely/go-kit/logger"
	"os"
)

type InterfaceWriter interface {
	Write(fileName string, array []string) (err error)
	WriteLine(fileName, array []string) (err error)
	Close()
}

type FileWriter struct {
	filePath string
	files    map[string]*os.File
	wfps     map[string]*bufio.Writer
	runLog   *logger.Logger
}

func NewFileWriter(
	filePath string,
	runLog *logger.Logger,
) *FileWriter {
	files := make(map[string]*os.File)
	wfps := make(map[string]*bufio.Writer)
	return &FileWriter{
		filePath: filePath,
		files:    files,
		wfps:     wfps,
		runLog:   runLog,
	}
}

func (w *FileWriter) Write(fileName string, array []string) (err error) {
	if len(array) == 0 {
		return nil
	}
	if _, ok := w.wfps[fileName]; !ok {
		file, err := os.OpenFile(fileName, os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		wfp := bufio.NewWriter(file)
		w.files[fileName] = file
		w.wfps[fileName] = wfp
	}
	return w.WriteLine(fileName, array)
}

func (w *FileWriter) WriteLine(fileName string, array []string) (err error) {
	for _, s := range array {
		_, _ = w.wfps[fileName].WriteString(s)
		_, _ = w.wfps[fileName].WriteString("\t")
	}
	_, err = w.wfps[fileName].WriteString("\n")
	return
}

func (w *FileWriter) Close() {
	for _, wfp := range w.wfps {
		if wfp != nil {
			_ = wfp.Flush()
			wfp = nil
		}
	}
	for _, file := range w.files {
		if file != nil {
			_ = file.Close()
			file = nil
		}
	}
}
