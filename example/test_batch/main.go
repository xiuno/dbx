package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
)


// create table foo (
// bar bigint,
// baz ascii,
// primary key (bar)
// );

// https://www.cnblogs.com/zhangqingping/p/4809876.html
/*
listen:
broadcast_rpc_address

 */
func main() {
	//session := *gocql.Session{}
	cluster := gocql.NewCluster("192.168.0.129:9042")
	cluster.Keyspace = "btc"

	con := 200
	step := 5000
	var err error
	sessions := make([]*gocql.Session, con)
	for k := 0; k < con; k++ {
		sessions[k], err = cluster.CreateSession()
		if err != nil {
			panic(err)
		}
	}
	t1 := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(con)
	for k := 0; k < con; k++ {
		go func(k int) {

			session := sessions[k]

			//err = session.Query("TRUNCATE user;").Exec()
			if err != nil {
				panic(err)
			}
			defer session.Close()

			for i := k * step; i < k * step + step; i++ {
				session.Query("INSERT INTO user (uid,gid) VALUES (?,?)", i, i).Exec()
			}
			wg.Done()
		}(k)
	}
	wg.Wait()


	//batch := session.NewBatch(gocql.UnloggedBatch)
	//for i := 0; i < 100; i++ {
	//	batch.Query("INSERT INTO user (uid,gid) VALUES (?,?)", i, i)
	//}
	//err = session.ExecuteBatch(batch)
	//if err != nil {
	//	panic(err)
	//}
	t2 := time.Now().Sub(t1)
	fmt.Printf("time: %v\n", t2.String())
}
//
////批量执行数据
//func TestScylla_Batch(t *testing.T) {
//	query := fmt.Sprintf(`BEGIN BATCH
//            UPDATE user SET user_name = 'asdqw' where id = %d;
//            INSERT INTO user (id,user_name) VALUES (2,'zhangsan');
//            APPLY BATCH;`, 1)
//	err := session.Query(query).Exec()
//	if err != nil {
//		fmt.Println(err)
//	}
//}
