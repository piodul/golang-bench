module github.com/PhazzaT/golang-bench

go 1.14

require (
	github.com/gocql/gocql v0.0.0-20201024154641-5913df4d474e
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.4.0
