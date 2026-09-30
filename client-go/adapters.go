package main

// Șablonul Adapter în Go: o interfață, două implementări (JSON, XML),
// ambele bazate pe bibliotecile standard encoding/json și encoding/xml.

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
)

// Message este modelul comun, independent de formatul de pe fir.
type Message struct {
	XMLName   xml.Name `xml:"message" json:"-"`
	Type      string   `xml:"type,omitempty" json:"type,omitempty"`
	ID        string   `xml:"id,omitempty" json:"id,omitempty"`
	Topic     string   `xml:"topic,omitempty" json:"topic,omitempty"`
	Sender    string   `xml:"sender,omitempty" json:"sender,omitempty"`
	Timestamp string   `xml:"timestamp,omitempty" json:"timestamp,omitempty"`
	Payload   string   `xml:"payload,omitempty" json:"payload,omitempty"`
	Checksum  string   `xml:"checksum,omitempty" json:"checksum,omitempty"`
	Format    string   `xml:"format,omitempty" json:"format,omitempty"`
	Role      string   `xml:"role,omitempty" json:"role,omitempty"`
	Ref       string   `xml:"ref,omitempty" json:"ref,omitempty"`
	Error     string   `xml:"error,omitempty" json:"error,omitempty"`
	Attempt   string   `xml:"attempt,omitempty" json:"attempt,omitempty"`
}

type MessageAdapter interface {
	Name() string
	Serialize(m *Message) ([]byte, error)
	Deserialize(data []byte) (*Message, error)
}

type JSONAdapter struct{}

func (JSONAdapter) Name() string                         { return "json" }
func (JSONAdapter) Serialize(m *Message) ([]byte, error) { return json.Marshal(m) }
func (JSONAdapter) Deserialize(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("JSON invalid: %w", err)
	}
	return &m, nil
}

type XMLAdapter struct{}

func (XMLAdapter) Name() string                         { return "xml" }
func (XMLAdapter) Serialize(m *Message) ([]byte, error) { return xml.Marshal(m) }
func (XMLAdapter) Deserialize(data []byte) (*Message, error) {
	if bytes.Contains(bytes.ToUpper(data), []byte("<!DOCTYPE")) {
		return nil, fmt.Errorf("XML cu DOCTYPE nu este permis")
	}
	var m Message
	if err := xml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("XML invalid: %w", err)
	}
	return &m, nil
}

var registry = map[string]MessageAdapter{"json": JSONAdapter{}, "xml": XMLAdapter{}}

func AdapterFor(name string) (MessageAdapter, error) {
	if a, ok := registry[name]; ok {
		return a, nil
	}
	return nil, fmt.Errorf("format necunoscut: %s", name)
}

func DetectAdapter(data []byte) (MessageAdapter, error) {
	t := bytes.TrimLeft(data, "\xef\xbb\xbf \t\r\n")
	switch {
	case len(t) > 0 && t[0] == '{':
		return registry["json"], nil
	case len(t) > 0 && t[0] == '<':
		return registry["xml"], nil
	}
	return nil, fmt.Errorf("format nedetectabil")
}
