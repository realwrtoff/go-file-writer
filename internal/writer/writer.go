package writer

import (
	"bufio"
	"github.com/hatlonely/go-kit/logger"
	"github.com/realwrtoff/go-file-writer/internal/public"
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
	fpsCount map[string]int
	runLog   *logger.Logger
}

func NewFileWriter(
	filePath string,
	runLog *logger.Logger,
) *FileWriter {
	files := make(map[string]*os.File)
	wfps := make(map[string]*bufio.Writer)
	fpsCount := make(map[string]int)
	return &FileWriter{
		filePath: filePath,
		files:    files,
		wfps:     wfps,
		fpsCount: fpsCount,
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
	err = public.CheckCreatePath(fileName)
	if err != nil {
		return err
	}
	if _, ok := w.wfps[fileName]; !ok {
		w.runLog.Infof("Open new file %s\n", fileName)
		file, err := os.OpenFile(fileName, os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		wfp := bufio.NewWriter(file)
		w.files[fileName] = file
		w.wfps[fileName] = wfp
		w.fpsCount[fileName] = 0
	}
	return w.WriteLine(fileName, buffArray)
}

func (w *FileWriter) WriteLine(fileName string, array []string) (err error) {
	for _, s := range array {
		_, _ = w.wfps[fileName].WriteString(s)
		_, _ = w.wfps[fileName].WriteString(",")
	}
	_, err = w.wfps[fileName].WriteString("\n")
	w.fpsCount[fileName] += 1
	if w.fpsCount[fileName]%10000 == 0 {
		err = w.wfps[fileName].Flush()
	}
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
