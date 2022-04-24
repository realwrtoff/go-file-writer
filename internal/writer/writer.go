package writer

import (
	"bufio"
	"github.com/hatlonely/go-kit/logger"
	"os"
)

type InterfaceWriter interface {
	GetFileName([]string) string
	Write([]string, []string) error
	WriteLine(string, []string) error
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

func (w *FileWriter) GetFileName(arrays []string) string {
	if len(arrays) == 0 {
		return w.filePath
	}
	fileName := w.filePath
	for _, k := range arrays {
		fileName += k
		fileName += "_"
	}
	fileName = fileName[:len(fileName)-1] + ".txt"
	return fileName
}

func (w *FileWriter) Write(keyArray []string, buffArray []string) (err error) {
	if len(buffArray) == 0 {
		return nil
	}
	fileName := w.GetFileName(keyArray)
	if _, ok := w.wfps[fileName]; !ok {
		file, err := os.OpenFile(fileName, os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		wfp := bufio.NewWriter(file)
		w.files[fileName] = file
		w.wfps[fileName] = wfp
	}
	return w.WriteLine(fileName, buffArray)
}

func (w *FileWriter) WriteLine(fileName string, array []string) (err error) {
	for _, s := range array {
		_, _ = w.wfps[fileName].WriteString(s)
		_, _ = w.wfps[fileName].WriteString(",")
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
