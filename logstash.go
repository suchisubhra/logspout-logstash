package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	mu    sync.Mutex
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	conn, err := connect(route)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

func connect(route *router.Route) (net.Conn, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	if reflect.TypeOf(conn).String() == "*net.TCPConn" {
		tcpconn := conn.(*net.TCPConn)
		err = tcpconn.SetKeepAlive(true)
		if err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (a *LogstashAdapter) maybeReconnect(writeErr error) error {
	time.Sleep(500 * time.Millisecond)
	conn, err := connect(a.route)
	if err != nil {
		return err
	}

	a.mu.Lock()
	a.conn.Close()
	a.conn = conn
	a.mu.Unlock()

	return nil
}

func envLookup(env []string, key string) string {
	for _, e := range env {
		data := strings.SplitN(e, "=", 2)
		if data[0] == key {
			return data[1]
		}
	}
	return ""
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	for m := range logstream {
		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}
		kubeInfo := KubernetesInfo{
			Pod: m.Container.Config.Labels["io.kubernetes.pod.name"],
		}
		var js []byte

		var jsonMsg map[string]interface{}
		err := json.Unmarshal([]byte(m.Data), &jsonMsg)
		if err != nil {
			// the message is not in JSON make a new JSON message
			msg := LogstashMessage{
				Message:    m.Data,
                                Stream:  m.Source,
				Docker:     dockerInfo,
				Kubernetes: kubeInfo,
			}
			js, err = json.Marshal(msg)
			js = append(js, '\n')
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		} else {
			// the message is already in JSON just add extra fields as a nested structures
			jsonMsg["docker"] = dockerInfo
                        data["stream"] = m.Source
			jsonMsg["kubernetes"] = kubeInfo

                        // Return the JSON encoding
			js, err = json.Marshal(jsonMsg)
			if err != nil {
				log.Println("logstash:", err)
				continue
			}
		}

		_, err = a.conn.Write(js)
		for err != nil {
			log.Println("logstash:", err)
			err = a.maybeReconnect(err)
			if err != nil {
				log.Println("logstash:", err)
				break
			}
			_, err = a.conn.Write(js)
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

type MesosInfo struct {
	TaskID string `json:"task_id"`
}

type KubernetesInfo struct {
	Pod string `json:"pod"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message    string     `json:"message"`
        Stream     string     `json:"stream"`
	Docker     DockerInfo `json:"docker"`
        Kubernetes KubernetesInfo `json:"kubernetes"`
}
