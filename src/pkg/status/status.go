package status

import (
	"strings"
	"strconv"
	"http"
	"template"
	"log"
	"io"
	"fmt"
	"container/vector"
)


type Counter struct {
	FValues map[string]float64
	IValues map[string]int
}

type Namespace struct {
	Namespace map[string]*Counter
}

type Leaf struct {
	K string
	V string
}

type TopLeaf struct {
	K string
	V *vector.Vector
}

var fmap = template.FormatterMap{
	"html": template.HTMLFormatter,
}

const (
	statsTemplatePath = "./template/stats.html"
)

var statsTempl = template.New(fmap)
var ns = &Namespace{Namespace: make(map[string]*Counter)}
var ws = Get("webstats")

// Get a counter Namespace
func Get(name string) *Counter {
	res := ns.Namespace[name]
	if res == nil {
		res = &Counter{IValues: make(map[string]int)}
		ns.Namespace[name] = res
	}
	return res
}

func (c *Counter) Inc(key string, amount int) {
	c.IValues[key] += amount
}


// Start serving traffic from this
func Start(addr string) {
	go Run(addr)
}

func Run(addr string) {
	// Configure the delimiters
	// Parse the file once
	statsTempl.SetDelims("<?", "?>")
	statsTempl.ParseFile(statsTemplatePath)

	http.Handle("/stats", http.HandlerFunc(Stats))
	http.Handle("/add/", http.HandlerFunc(Add))
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

func Add(w http.ResponseWriter, req *http.Request) {
	strs := strings.Split(req.URL.Path, "/", 4)
	if len(strs) != 4 {
		w.Write([]byte(fmt.Sprintf("Failure, not enough parts, need 4 found %d", len(strs))))
		return
	}
	amount, err := strconv.Atoi(strs[3])
	if err != nil {
		amount = 1
	}
	ws.Inc(strs[2], amount)
	w.Write([]byte("OK"))
}

// Ideally I'd like to find a better way to serialize this..
func Stats(w http.ResponseWriter, req *http.Request) {
	results := make([]*TopLeaf, 0, len(ns.Namespace))
	innerResults := new(vector.Vector)
	for k, v := range ns.Namespace {
		for ik, iv := range v.IValues {
			innerResults.Push(&Leaf{ik, fmt.Sprintf("%v", iv)})
		}
		results = append(results, &TopLeaf{k, innerResults})
		innerResults = new(vector.Vector)
	}
	statsTempl.Execute(w, results)
}

func UrlHtmlFormatter(w io.Writer, fmt string, v ...interface{}) {
	template.HTMLEscape(w, []byte(http.URLEscape(v[0].(string))))
}
