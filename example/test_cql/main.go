package main

import (
	"fmt"
	"os"
	"time"

	"github.com/xiuno/dbx"
)

type User struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	Amount     float64   `db:"amount""`
	CreateDate time.Time `db:"create_date"`
}
/*

CREATE TABLE IF NOT EXISTS user(uid        INT, gid        INT, name       TEXT, amount     DOUBLE, create_date TIMESTAMP, PRIMARY KEY(uid, gid));
insert into user (uid, gid, name, amount) values (1, 2, 'name-1', 0.1);
insert into user (uid, gid, name, amount) values (2, 2, 'name-2', 0.2);
insert into user (uid, gid, name, amount) values (3, 2, 'name-3', 0.3);
insert into user (uid, gid, name, amount) values (3, 5, 'name-5', 0.5);
replace into user (uid, gid, name, amount) values (4, 5, 'name-5', 0.5);


CREATE TABLE IF NOT EXISTS user_addr(uid        INT, addr        text, PRIMARY KEY(uid, addr));
insert into user_addr (uid,addr) values (1, 'addr1-1');
insert into user_addr (uid, addr) values (1, 'addr1-2');
insert into user_addr (uid, addr) values (1, 'addr1-3');
insert into user_addr (uid, addr) values (2, 'addr2-1');
insert into user_addr (uid, addr) values (2, 'addr2-2');
insert into user_addr (uid, addr) values (2, 'addr2-3');
 */

func main() {

	var err error
	var db *dbx.DB

	//db, err = dbx.Open("cql", "root@tcp(192.168.0.129:9042)/btc")
	db, err = dbx.Open("cql", "root@tcp(192.168.0.129:9042)/btc")
	dbx.Check(err)
	defer db.Close()

	// db 输出信息设置
	db.Stdout = os.Stdout                       // 默认：将 db 产生的错误信息输出到标准输出
	db.Stderr = dbx.OpenFile("./db1_error.log") // 将 db 产生的错误信息输出到指定的文件
	// db.Stdout = ioutil.Discard // 默认：将 db 的输出信息重定向到"黑洞"（不输出执行的 SQL 语句等信息）

	// 参数设置

	// 创建表
	//_, err = db.Exec(`DROP TABLE IF EXISTS user;`)
	//_, err = db.Exec(`CREATE TABLE IF NOT EXISTS user(uid        INT, gid        INT, name       TEXT, amount     DOUBLE, create_date TIMESTAMP, PRIMARY KEY(uid));`)

	//err = db.CQLSession.Query("insert into user (uid, gid) values (1, 2);").Exec()
	//err = db.CQLSession.Query("insert into user (uid, gid) values (2, 2);").Exec()
	//if err != nil {
	//	panic(err)
	//}
//	iter := db.CQLSession.Query("SELECT uid FROM user").Iter()
	//collist := iter.Columns()
	//for _, v := range collist {
	//	fmt.Printf("%v", v.Name)
	//}


	//values := make([]interface{}, 1)
	//
	//var uid2 int64;
	//values[0] = reflect.New(reflect.TypeOf(uid2)).Interface()
	////values[0] = new(interface{})
	////for iter.Scan(values...) {
	//for iter.Scan(values...) {
	//	//fmt.Printf("v: %v\n", values[0])
	//	b := reflect.ValueOf(values[0]).Elem().Interface()
	//	fmt.Printf("v: %v\n", b)
	//}
	//
	// uid is called the partition key and gid, ... are called clustering keys.
	// where partid=123 and createDate>'2018-1-1'
	dbx.Check(err)

	// 开启缓存，可选项，一般只针对小表开启缓存，超过 10w 行，不建议开启！
	db.Bind("user", &User{}, true)
	db.EnableCache(true)

	// 插入一条
	u1 := &User{1, 1, "jet", 1.2, time.Now()}
	_, err = db.Table("user").Insert(u1)
	dbx.Check(err)

	// 读取一条
	u2 := &User{}
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
	fmt.Printf("%+v\n", u2)

	// 读取一条，判断是否存在
	err = db.Table("user").WherePK(1).One(u2)
	dbx.Check(err)
	fmt.Printf("%+v\n", u2)

	// 日期
	err = db.Table("user").Where("createDate>?", time.Now().Add(-10 * time.Second).Format("2006-01-02 03:04:05")).One(u2)
	dbx.Check(err)
	fmt.Printf("%+v\n", u2)

	// 更新一条
	u2.Name = "jet.li"
	_, err = db.Table("user").Update(u2)
	dbx.Check(err)

	// Where 条件 + 更新
	_, err = db.Table("user").WhereM(dbx.M{{"uid", 1}, {"gid", 1}}).UpdateM(dbx.M{{"name", "jet.li"}})
	dbx.Check(err)

	// 删除一条
	_, err = db.Table("user").WherePK(1).Delete()
	dbx.Check(err)

	// 插入多条
	for i := int64(0); i < 5; i++ {
		u := &User{
			Uid:        i,
			Gid:        i,
			Name:       fmt.Sprintf("name-%v", i),
			CreateDate: time.Now(),
		}
		//time.Sleep(10 * time.Second)
		_, err := db.Table("user").Insert(u)
		if err != nil {
			//fmt.Printf("%v\n", err.Error())
			panic(err)
		}
	}

	// 批量插入
	//BEGIN BATCH … APPLY BATCH
	// 获取多条
	userList := []*User{}

	// 获取多条无结果
	err = db.Table("user").Where("uid>?", 1000).All(&userList)
	dbx.Check(err)

	err = db.Table("user").Where("uid>?", 1).All(&userList)
	dbx.Check(err)
	for _, u := range userList {
		fmt.Printf("%+v\n", u)
	}

	// 批量更新
	_, err = db.Table("user").Where("uid>?", 3).UpdateM(dbx.M{{"gid", 10}})
	dbx.Check(err)

	// 批量删除
	_, err = db.Table("user").Where("uid>?", 3).Delete()
	dbx.Check(err)

	// 总数
	n, err := db.Table("user").Where("uid>?", -1).Count()
	dbx.Check(err)
	fmt.Printf("count: %v\n", n)

	// 求和
	n, err = db.Table("user").Where("uid>?", -1).Sum("uid")
	dbx.Check(err)
	fmt.Printf("sum(uid): %v\n", n)

	// 求最大值
	n, err = db.Table("user").Where("uid>?", -1).Max("uid")
	dbx.Check(err)
	fmt.Printf("max(uid): %v\n", n)

	// 求最小值
	n, err = db.Table("user").Where("uid>?", -1).Min("uid")
	dbx.Check(err)
	fmt.Printf("min(uid): %v\n", n)

	// 自定义复杂 SQL 获取单条结果（原生）
	var uid int64
	err = db.CQLSession.Query("SELECT uid FROM user WHERE uid=?", 2).Scan(&uid)
	dbx.Check(err)
	fmt.Printf("uid: %v\n", uid)
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 自定义复杂 SQL 获取多条（原生）
	var name string
	rows := db.CQLSession.Query("SELECT `uid`, `name` FROM `user` WHERE 1 ORDER BY uid DESC").Iter()
	dbx.Check(err)
	rows.Close()
	for rows.Scan(&uid, &name) {
		fmt.Printf("uid: %v, name: %v\n", uid, name)
	}
	db.Table("user").LoadCache() // 自定义需要手动刷新缓存

	// 其他
	//userlist := []*User{}
	mp := db.Table("user").AllFromCache()
	fmt.Printf("mp.Len(): %v\n", mp.Len())

	//userlist := []*User{}
	n, err = db.Table("user").Count()
	fmt.Printf("count: %v\n", n)

	// gid+1
	u := &User{
		Uid:        300,
		Gid:        300,
		Name:       fmt.Sprintf("name-%v", 300),
		CreateDate: time.Now(),
	}

	_, err = db.Table("user").Replace(u)
	dbx.Check(err)

	// 只能对 Counter 列进行加减！
	// Invalid operation gid=gid+1 for non counter column
	db.Table("user").WherePK(300).UpdateM(dbx.M{{"gid+", 1}})
	err = db.Table("user").WherePK(300).One(u1)
	dbx.Check(err)
	if u1.Gid != 301 {
		panic("gid error.")
	}

	// 必须指定主键才能删除！
	db.Table("user").WherePK(1).Delete()
	err = db.Table("user").WherePK(1).One(u1)
	dbx.Check(err)
	if !dbx.NoRows(err) {
		panic("delete error.")
	}

	return
}
