package test

import (
	"fmt"
	"github.com/xiuno/dbx"
	"gotest.tools/assert"
	"os"
	"testing"
	"time"
)

var db *dbx.DB
var err error

type User struct {
	Uid        int64     `db:"uid"`
	Gid        int64     `db:"gid"`
	Name       string    `db:"name"`
	CreateDate time.Time `db:"createDate"`
}

func initMySQL() {

	db, err = dbx.Open("mysql", "root@tcp(localhost)/test?parseTime=true&charset=utf8")
	if err != nil {
		panic(err)
	}
	//defer db.Close()

	db.Stdout = os.Stdout
	db.Stderr = os.Stdout

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	//db.SetConnMaxLifetime(time.Second * 60) // 不设置一直保持

	_, err = db.Exec(`DROP TABLE IF EXISTS user;`)
	_, err = db.Exec(`CREATE TABLE user(
		uid        INT(11) PRIMARY KEY AUTO_INCREMENT,
		gid        INT(11) NOT NULL DEFAULT '0',
		name       TEXT             DEFAULT '',
		createDate DATETIME         DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		panic(err)
	}

	// 开启缓存，可选项，一般只针对小表开启缓存，超过 10w 行，不建议开启！
	db.Bind("user", &User{}, true)
	db.EnableCache(true)

}

func TestOne(t *testing.T) {

	initMySQL()

	var n int64

	// 时间这里有坑，格式化以后的时间可能丢掉微秒
	now := time.Now()

	// 插入一条
	u1 := &User{1, 1, "jet", now}
	_, err = db.Table("user").Insert(u1)
	assert.Equal(t, err, nil)

	// 取出来，进行比较
	u2 := &User{}
	db.Table("user").WherePK(1).One(u2)
	assert.Equal(t, *u1, *u2)

	// 更新
	u2.Gid = 2
	u2.Name = "jet2"
	u2.CreateDate = now.Add(1 * time.Second)
	n, err = db.Table("user").WherePK(1).Update(u2)
	assert.Equal(t, n, int64(1))
	assert.Equal(t, err, nil)

	// 取出来，进行比较
	u3 := &User{}
	db.Table("user").WherePK(1).One(u3)
	assert.Equal(t, u2.Name, "jet2")


	n, err = db.Table("user").WherePK(1).UpdateM(dbx.M{{"name", "jet3"}})
	assert.Equal(t, n, int64(1))
	assert.Equal(t, err, nil)

	// 取出来，进行比较
	db.Table("user").WherePK(1).One(u3)
	assert.Equal(t, u3.Name, "jet3")

	// 其他条件的查询测试
	err = db.Table("user").Where("uid=?", 1).One(u2)
	assert.Equal(t, err, nil)
	assert.Equal(t, u2.Uid, u3.Uid)

	db.Table("user").WhereM(dbx.M{{"uid", 1}}).One(u3)
	assert.Equal(t, *u3, *u2)

	db.Table("user").WhereM(dbx.M{{"uid", 0}}).One(u3)
	assert.Equal(t, *u3, *u2)

	db.Table("user").WhereM(dbx.M{{"gid", 0}}).One(u3)
	assert.Equal(t, *u3, *u2)

	db.Table("user").WhereM(dbx.M{{"uid", 1}, {"gid", 1}}).One(u3)
	assert.Equal(t, *u3, *u2)

	// 删除
	n, err = db.Table("user").WherePK(1).Delete()
	assert.Equal(t, n, int64(1))
	assert.Equal(t, err, nil)

	// 再次查询
	err = db.Table("user").WherePK(1).One(u3)
	assert.Equal(t, err, dbx.ErrNoRows)

	// 日期查询

}


func TestMulti(t *testing.T) {

	initMySQL()

	// 插入多条
	var n int64
	var err error

	err = db.Table("user").Truncate()
	assert.Equal(t, err, nil)

	now := time.Now()
	for i := int64(1); i < 5; i++ {
		u := &User{
			Uid: i,
			Gid: i,
			Name: fmt.Sprintf("name-%v", i),
			CreateDate: now,
		}
		n, err = db.Table("user").Insert(u)
		assert.Equal(t, err, nil)
		assert.Equal(t, n, i)
	}
	for i := int64(1); i < 5; i++ {
		u2 := &User{}
		err = db.Table("user").WherePK(i).One(u2)
		assert.Equal(t, err, nil)
		assert.Equal(t, u2.Uid, i)
		assert.Equal(t, u2.Gid, i)
		assert.Equal(t, u2.Name, fmt.Sprintf("name-%v", i))
		assert.Equal(t, u2.CreateDate, now)
	}


	// 复杂条件查询一条
	u3 := &User{}
	err = db.Table("user").Where("uid=? AND gid=?", 1, 1).Sort("uid", 1).One(u3)
	assert.Equal(t, err, nil)
	assert.Equal(t, u3.Uid, int64(1))

	// 复杂条件查询多条
	userList := []*User{}
	err = db.Table("user").Where("uid>? AND gid>?", 0, 0).Sort("uid", 1).Limit(0, 2).All(&userList)
	assert.Equal(t, userList[0].Uid, int64(1))
	assert.Equal(t, userList[1].Uid, int64(2))

	// 复杂条件查询多条
	userList2 := []User{}
	err = db.Table("user").Where("uid>? AND gid>?", 0, 0).Sort("uid", 1).Limit(0, 2).All(&userList2)
	assert.Equal(t, userList2[0].Uid, int64(1))
	assert.Equal(t, userList2[1].Uid, int64(2))

	// 复杂条件更新多条
	n, err = db.Table("user").Where("uid>? AND gid>?", 0, 0).Sort("uid", 1).UpdateM(dbx.M{{"name", "jet3"}})
	assert.Equal(t, err, nil)
	assert.Equal(t, n, int64(4))

	// 校验 DB
	err = db.Table("user").Where("uid=?", 1).One(u3)
	assert.Equal(t, err, nil)
	assert.Equal(t, u3.Name, "jet3")

	// 校验 Cache
	err = db.Table("user").WherePK(1).One(u3)
	assert.Equal(t, err, nil)
	assert.Equal(t, u3.Name, "jet3")


	// 插入 InsertIgnore
	u1 := &User{0, 222, "Jack", now}
	_, err = db.Table("user").InsertIgnore(u1)
	assert.Equal(t, err, nil)
	// 取出来，进行比较
	u2 := &User{}
	db.Table("user").Where("gid=?", 222).One(u2)
	assert.Equal(t, u2.Gid, int64(222))

	// 最小值
	n, err = db.Table("user").Min("gid")
	assert.Equal(t, err, nil)
	assert.Equal(t, n, int64(1))

	// 最大值
	n, err = db.Table("user").Max("gid")
	assert.Equal(t, err, nil)
	assert.Equal(t, n, int64(222))

	// 复杂条件查询
	err = db.Table("user").Where("uid>? AND gid>? GROUP BY uid", 0, 0).All(&userList)
	assert.Equal(t, err, nil)
	assert.Equal(t, 5, len(userList))

	// 复杂条件删除多条
	n, err = db.Table("user").Where("uid>? AND gid>?", 0, 0).Sort("uid", 1).Delete()
	assert.Equal(t, err, nil)
	assert.Equal(t, n, int64(5))


}
