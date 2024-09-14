package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

type Job struct {
	ID     int         `json:"id"`
	Queue  string      `json:"queue"`
	Pri    int         `json:"pri"`
	Job    interface{} `json:"job"`
	Client net.Conn    `json:"-"`
}

type PriorityQueue struct {
	jobs []*Job
	mu   sync.Mutex
}

func (pq *PriorityQueue) Len() int {
	return len(pq.jobs)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.jobs[i].Pri > pq.jobs[j].Pri // Higher priority comes first
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.jobs[i], pq.jobs[j] = pq.jobs[j], pq.jobs[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	pq.jobs = append(pq.jobs, x.(*Job))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.jobs
	n := len(old)
	item := old[n-1]
	pq.jobs = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Init() {
	heap.Init(pq)
}

func (pq *PriorityQueue) PushJob(job *Job) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq, job)
}

func (pq *PriorityQueue) PopJob() *Job {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.Len() == 0 {
		return nil
	}
	return heap.Pop(pq).(*Job)
}

type Server struct {
	queues         map[string]*PriorityQueue
	jobs           map[int]*Job
	workingJobs    map[int]net.Conn
	mu             sync.Mutex
	nextJobID      int
	waitingClients map[string][]chan *Job
}

func NewServer() *Server {
	return &Server{
		queues:         make(map[string]*PriorityQueue),
		jobs:           make(map[int]*Job),
		workingJobs:    make(map[int]net.Conn),
		nextJobID:      1,
		waitingClients: make(map[string][]chan *Job),
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in handleConnection: %v", r)
			s.sendErrorResponse(conn, "Internal server error")
		}
		s.handleDisconnect(conn)
		conn.Close()
	}()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var req map[string]interface{}
		if err := decoder.Decode(&req); err != nil {
			s.sendErrorResponse(conn, "Invalid JSON request")
			return
		}

		s.mu.Lock()
		var resp interface{}
		var err error
		switch req["request"] {
		case "put":
			resp, err = s.handlePut(req)
		case "get":
			resp, err = s.handleGet(req, conn)
		case "delete":
			resp, err = s.handleDelete(req)
		case "abort":
			resp, err = s.handleAbort(req, conn)
		default:
			err = fmt.Errorf("Unrecognized request type")
		}
		s.mu.Unlock()

		if err != nil {
			s.sendErrorResponse(conn, err.Error())
		} else if err := encoder.Encode(resp); err != nil {
			log.Printf("Error encoding response: %v", err)
			return
		}
	}
}

func (s *Server) sendErrorResponse(conn net.Conn, message string) {
	response := map[string]string{"status": "error", "error": message}
	if err := json.NewEncoder(conn).Encode(response); err != nil {
		log.Printf("Error sending error response: %v", err)
	}
}

func (s *Server) handleDisconnect(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for jobID, clientConn := range s.workingJobs {
		if clientConn == conn {
			s.abortJob(jobID)
		}
	}
}

func (s *Server) abortJob(jobID int) {
	if job, ok := s.jobs[jobID]; ok {
		delete(s.workingJobs, jobID)
		s.queues[job.Queue].PushJob(job)
		log.Printf("Job %d aborted and returned to queue %s", jobID, job.Queue)

		// Notify waiting clients
		s.notifyWaitingClients(job.Queue)
	}
}

func (s *Server) notifyWaitingClients(queue string) {
	if waiting, ok := s.waitingClients[queue]; ok {
		for _, ch := range waiting {
			select {
			case ch <- nil: // Notify the client that a job is available
			default: // If the channel is full, skip it
			}
		}
		s.waitingClients[queue] = nil // Clear the waiting list
	}
}

func (s *Server) handlePut(req map[string]interface{}) (map[string]interface{}, error) {
	queueName, ok := req["queue"].(string)
	if !ok {
		return nil, fmt.Errorf("Invalid queue name")
	}
	jobData, ok := req["job"]
	if !ok {
		return nil, fmt.Errorf("Missing job data")
	}
	priority, ok := req["pri"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid priority")
	}

	job := &Job{
		ID:    s.nextJobID,
		Queue: queueName,
		Pri:   int(priority),
		Job:   jobData,
	}

	s.nextJobID++
	if _, ok := s.queues[queueName]; !ok {
		s.queues[queueName] = &PriorityQueue{}
		s.queues[queueName].Init()
	}
	s.queues[queueName].PushJob(job)
	s.jobs[job.ID] = job

	log.Printf("Job %d added to queue %s with priority %d", job.ID, queueName, job.Pri)
	s.notifyWaitingClients(queueName)
	return map[string]interface{}{"status": "ok", "id": job.ID}, nil
}

func (s *Server) handleGet(req map[string]interface{}, conn net.Conn) (map[string]interface{}, error) {
	queues, ok := req["queues"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("Invalid queues list")
	}
	wait, _ := req["wait"].(bool)

	var job *Job
	for {
		job = s.getHighestPriorityJob(queues)
		if job != nil {
			break
		}
		if !wait {
			return map[string]interface{}{"status": "no-job"}, nil
		}

		// Wait for a job to become available
		ch := make(chan *Job, 1)
		for _, q := range queues {
			queueName := q.(string)
			s.waitingClients[queueName] = append(s.waitingClients[queueName], ch)
		}
		s.mu.Unlock()
		<-ch // Wait for notification
		s.mu.Lock()
	}

	s.workingJobs[job.ID] = conn
	log.Printf("Job %d retrieved from queue %s with priority %d", job.ID, job.Queue, job.Pri)
	return map[string]interface{}{
		"status": "ok", "id": job.ID, "job": job.Job, "pri": job.Pri, "queue": job.Queue,
	}, nil
}

func (s *Server) getHighestPriorityJob(queues []interface{}) *Job {
	var highestPriorityJob *Job
	var selectedQueue string
	var selectedIndex int

	for _, q := range queues {
		queueName := q.(string)
		if pq, ok := s.queues[queueName]; ok {
			pq.mu.Lock()
			for i, job := range pq.jobs {
				if _, exists := s.jobs[job.ID]; exists {
					if highestPriorityJob == nil || job.Pri > highestPriorityJob.Pri {
						highestPriorityJob = job
						selectedQueue = queueName
						selectedIndex = i
					}
					break
				}
			}
			pq.mu.Unlock()
		}
	}

	if highestPriorityJob != nil {
		// Remove the job from its queue
		pq := s.queues[selectedQueue]
		pq.mu.Lock()
		heap.Remove(pq, selectedIndex)
		pq.mu.Unlock()
		return highestPriorityJob
	}
	return nil
}

func (s *Server) handleDelete(req map[string]interface{}) (map[string]string, error) {
	jobID, ok := req["id"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid job ID")
	}

	if job, ok := s.jobs[int(jobID)]; ok {
		delete(s.jobs, int(jobID))
		delete(s.workingJobs, int(jobID))

		if pq, ok := s.queues[job.Queue]; ok {
			pq.mu.Lock()
			for i, j := range pq.jobs {
				if j.ID == int(jobID) {
					heap.Remove(pq, i)
					break
				}
			}
			pq.mu.Unlock()
		}

		log.Printf("Job %d deleted", int(jobID))
		return map[string]string{"status": "ok"}, nil
	} else {
		return map[string]string{"status": "no-job"}, nil
	}
}

func (s *Server) handleAbort(req map[string]interface{}, conn net.Conn) (map[string]string, error) {
	jobID, ok := req["id"].(float64)
	if !ok {
		return nil, fmt.Errorf("Invalid job ID")
	}

	if _, ok := s.jobs[int(jobID)]; ok {
		if s.workingJobs[int(jobID)] == conn {
			s.abortJob(int(jobID))
			return map[string]string{"status": "ok"}, nil
		} else {
			return nil, fmt.Errorf("Not authorized to abort this job")
		}
	} else {
		return map[string]string{"status": "no-job"}, nil
	}
}

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting job queue server...")

	server := NewServer()
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer ln.Close()

	log.Println("Server started on port 8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go server.handleConnection(conn)
	}
}
