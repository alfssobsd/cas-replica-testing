package main

import (
	"flag"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"github.com/gocql/gocql"
	"log"
	"os"
)

func main() {

	argServer := flag.String("server", "127.0.0.1", "ex: 192.168.1.1")
	argKeyspace := flag.String("keyspace", "example", "keyspace name")
	argUsername := flag.String("username", "user", "Username for auth")
	argPassword := flag.String("password", "password", "Password for auth")
	argNeedAuth := flag.Bool("enableAuth", false, "enable auth, pls use with username, password flags")

	argStartAt := flag.Int("startAt", 1, "test records start number")
	argFinishAt := flag.Int("finishAt", 1000000, "test records finish number")

	flag.Parse()

	fmt.Printf("Run with options: \n")
	fmt.Printf("  server: %s:9042\n", *argServer)
	fmt.Printf("  keyspace: %v\n", *argKeyspace)
	fmt.Printf("  enable auth: %v\n", *argNeedAuth)
	if *argNeedAuth == true {
		fmt.Printf("  username: %v\n", *argUsername)
		fmt.Printf("  password: %v\n", *argPassword)
	}
	fmt.Printf("  startAt: %v\n", *argStartAt)
	fmt.Printf("  finishAt: %v\n", *argFinishAt)

	cluster := gocql.NewCluster(*argServer)
	if *argNeedAuth == true {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *argUsername,
			Password: *argPassword,
		}
	}
	cluster.Keyspace = *argKeyspace
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println("Can't connect to server: ", err)
		os.Exit(1)
	}
	defer session.Close()

	//Create table
	err = session.Query(`CREATE TABLE IF NOT EXISTS  emp(    emp_id int PRIMARY KEY,    emp_name text,    emp_city text,    emp_sal varint,    emp_phone varint    );`).Exec()
	if err != nil {
		log.Println("Can't create table emp: ", err)
	}

	//Progress bar
	fmt.Printf("Start generation records from %d to %d\n", *argStartAt, *argFinishAt)
	bar := pb.StartNew(*argFinishAt + 1 - *argStartAt)
	//Create test records
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.SetConsistency(gocql.One)
	for i := *argStartAt; i <= *argFinishAt; i++ {
		bar.Increment()
		batch.Query(`INSERT INTO emp (emp_id, emp_name, emp_city, emp_sal, emp_phone) VALUES (?,?,?,?,?)`,
			i, fmt.Sprintf("emploey%d", i), fmt.Sprintf("city%d", i), i, 70000000)
		if i%150 == 0 {
			err = session.ExecuteBatch(batch)
			if err != nil {
				log.Println("Error insert : ", err)
			}
			batch = session.NewBatch(gocql.LoggedBatch)
			batch.SetConsistency(gocql.One)
		}
	}

	err = session.ExecuteBatch(batch)
	if err != nil {
		log.Println("Error insert : ", err)
	}

	bar.Finish()
}
