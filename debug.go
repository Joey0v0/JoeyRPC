// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package joeyrpc

import (
	"html/template"
	"net/http"
	"sort"
)

var servicePage = template.Must(template.New("services").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>JoeyRPC Services</title></head>
<body><h1>JoeyRPC Services</h1>
{{range .}}<section><h2>Service {{.Name}}</h2>
<table><thead><tr><th>Method</th><th>Calls</th></tr></thead><tbody>
{{range .Methods}}<tr><td>{{.Name}}({{.Argument}}, {{.Reply}}) error</td><td>{{.Calls}}</td></tr>{{end}}
</tbody></table></section>{{end}}</body></html>`))

type debugHTTP struct{ *Server }
type methodView struct {
	Name, Argument, Reply string
	Calls                 uint64
}
type serviceView struct {
	Name    string
	Methods []methodView
}

// Build an ordered value snapshot before rendering. Templates never traverse
// the live catalog or invoke methods on internal mutable counters.
func (s debugHTTP) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	s.mu.RLock()
	views := make([]serviceView, 0, len(s.services))
	for name, catalog := range s.services {
		view := serviceView{Name: name}
		for methodName, method := range catalog.method {
			view.Methods = append(view.Methods, methodView{
				Name: methodName, Argument: method.ArgType.String(), Reply: method.ReplyType.String(), Calls: method.NumCalls(),
			})
		}
		sort.Slice(view.Methods, func(i, j int) bool { return view.Methods[i].Name < view.Methods[j].Name })
		views = append(views, view)
	}
	s.mu.RUnlock()
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := servicePage.Execute(w, views); err != nil {
		return
	}
}
