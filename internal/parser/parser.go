package parser

import (
	"github.com/sirupsen/logrus"
	"strings"
)


type InterfaceParser interface {
	Parse(line string) (key string, array []string, err error)
}

type Parser struct {
	fileType string
	runLog *logrus.Logger
}

func NewParser(
	fileType string,
	runLog *logrus.Logger,
) *Parser {
	return &Parser{
		fileType: fileType,
		runLog:   runLog,
	}
}

func (p *Parser)Parse(line string)(key string, array []string, err error)  {
	if p.fileType == "tuvssh" {
		array = strings.Split(line, "\t")
		key = array[1]
	}
	return
}