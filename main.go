package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"
)

type mode int

const (
	bmWriteOnly mode = iota
	bmReadAndWrite
	bmReadOnly
)

var (
	nodes             string
	concurrency       int
	tasks             int64
	replicationFactor int
	consistency       string
	compression       string

	workload string

	prepareMode bool
	benchMode   bool
)

func main() {
	flag.StringVar(&nodes, "nodes", "127.0.0.1", "comma-separated list of cluster contact nodes")
	flag.IntVar(&concurrency, "concurrency", 256, "workload concurrency")
	flag.Int64Var(&tasks, "tasks", 1000*1000, "task count")
	flag.IntVar(&replicationFactor, "replication-factor", 3, "replication factor")
	flag.StringVar(&consistency, "consistency", "quorum", "consistency level")
	flag.StringVar(&compression, "compression", "none", "compression algorithm to use (none or snappy")

	flag.StringVar(&workload, "workload", "write", "workload type (write, read or mixed)")

	flag.BoolVar(&prepareMode, "prepare", false, "prepare keyspace and table before the bench")
	flag.BoolVar(&benchMode, "run", false, "run the benchmark")

	flag.Parse()

	if prepareMode == benchMode {
		panic(errors.New("please specify exactly one of the following flags: -prepare, -run"))
	}

	cluster := gocql.NewCluster(strings.Split(nodes, ",")...)
	// This also enables shard-awareness
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Consistency = parseConsistency(consistency)
	cluster.Compressor = parseCompression(compression)
	cluster.Timeout = 5 * time.Second

	sess, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	if prepareMode {
		setupSchema(sess)
	} else if benchMode {
		runBench(sess)
	}
}

func setupSchema(sess *gocql.Session) {
	if err := sess.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS ks_golang_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}", replicationFactor)).Exec(); err != nil {
		panic(err)
	}
	if err := sess.Query("DROP TABLE IF EXISTS ks_golang_bench.t").Exec(); err != nil {
		panic(err)
	}
	if err := sess.Query("CREATE TABLE ks_golang_bench.t (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)").Exec(); err != nil {
		panic(err)
	}

	fmt.Println("Schema set up!")
}

func runBench(sess *gocql.Session) {
	mode := parseWorkloadType(workload)

	if mode == bmReadAndWrite {
		// In this mode we are writing and then reading from the same row,
		// so cut the amount of tasks in half
		tasks /= 2
	}

	commandC := make(chan int64, concurrency)

	errG, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < concurrency; i++ {
		errG.Go(func() error {
			readQ := sess.Query("SELECT v1, v2 FROM ks_golang_bench.t WHERE pk = ?")
			writeQ := sess.Query("INSERT INTO ks_golang_bench.t (pk, v1, v2) VALUES (?, ?, ?)")

			var v1 int64
			var v2 int64
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case i, ok := <-commandC:
					if !ok {
						// No more jobs
						return nil
					}
					if mode == bmWriteOnly || mode == bmReadAndWrite {
						if err := writeQ.Bind(i, 2*i, 3*i).Exec(); err != nil {
							return fmt.Errorf("exception while writing: %d %s", i, err)
						}
					}
					if mode == bmReadAndWrite || mode == bmReadOnly {
						if err := readQ.Bind(i).Scan(&v1, &v2); err != nil {
							return fmt.Errorf("exception while reading %d: %s", i, err)
						}
						if v1 != 2*i || v2 != 3*i {
							return fmt.Errorf("bad data; pk: %d, v1: %d, v2: %d", i, v1, v2)
						}
					}
				}
			}
		})
	}

	errG.Go(func() error {
		prevPercent := int64(-1)

		for i := int64(0); i < tasks; i++ {
			currPercent := (100 * i) / tasks
			if prevPercent < currPercent {
				prevPercent = currPercent
				fmt.Printf("Progress: %d%%\n", currPercent)
			}

			select {
			case commandC <- i:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		close(commandC)
		return nil
	})

	if err := errG.Wait(); err != nil {
		panic(err)
	}

	fmt.Println("Done!")
}

func parseConsistency(consistency string) gocql.Consistency {
	switch consistency {
	case "any":
		return gocql.Any
	case "one":
		return gocql.One
	case "quorum":
		return gocql.Quorum
	case "all":
		return gocql.All
	default:
		panic(fmt.Errorf("bad consistency: %s", consistency))
	}
}

func parseCompression(compression string) gocql.Compressor {
	switch compression {
	case "none":
		return nil
	case "snappy":
		return gocql.SnappyCompressor{}
	default:
		panic(fmt.Errorf("bad compression: %s", compression))
	}
}

func parseWorkloadType(workloadType string) mode {
	switch workloadType {
	case "write":
		return bmWriteOnly
	case "read":
		return bmReadOnly
	case "mixed":
		return bmReadAndWrite
	default:
		panic(fmt.Errorf("bad workload type: %s", workloadType))
	}
}
