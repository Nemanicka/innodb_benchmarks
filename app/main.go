package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	//    "time"

	_ "github.com/go-sql-driver/mysql"
)

var rowsCount = 5000000

func deleteData(ctx context.Context, conn *sql.Conn, tableName string) {

	_, deleteError := conn.QueryContext(ctx, `DELETE FROM `+tableName)
	if deleteError != nil {
		log.Fatal(deleteError)
	}
}

func prepareStmt(ctx context.Context, conn *sql.Conn, tableName string) *sql.Stmt {
	stmtIns, err := conn.PrepareContext(ctx, "INSERT INTO "+tableName+" VALUES( ?, ? )")
	if err != nil {
		log.Fatal(err.Error(), " | prepare error")
	}
	return stmtIns
}

func insertData(stmtIns *sql.Stmt) {
	date := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < rowsCount; i++ {
		date = date.Add(1 * time.Minute)
		if i%10000 == 0 {
			fmt.Println(i, date)
		}
		_, err := stmtIns.Exec(i, date)
		if err != nil {
			if strings.Contains(err.Error(), "Duplicate entry") {
				fmt.Println("Duplicate", i)
				continue
			}
			log.Fatal(err.Error(), " | insert error")
		}
	}
}

// /
// /Insert Execution time:
// No Index   :  2180.6530592
// Hash Index :  2193.4942367
// Btree Index:  2283.5473962
func checkRowsCount(ctx context.Context, conn *sql.Conn, tableName string) {
	var actualRowsCount int
	err := conn.QueryRowContext(ctx, "Select Count(id) FROM "+tableName).Scan(&actualRowsCount)
	if err != nil {
		log.Fatal(err)
	}

	if actualRowsCount != int(rowsCount) {
		log.Fatalln("Expected rows count:", actualRowsCount, ", actual rows count:", rowsCount)
	}
}

func createData(ctx context.Context, conn *sql.Conn) {
	_, tableCreateError := conn.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS UsersNoIndex ( id INT UNIQUE, birthDate DATE)")
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	_, tableCreateError = conn.QueryContext(ctx, `CREATE TABLE IF NOT EXISTS UsersHashIndex ( id INT UNIQUE, 
        birthDate DATE,  INDEX birthDate_hash (birthDate) USING HASH)`)
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	_, tableCreateError = conn.QueryContext(ctx, `CREATE TABLE IF NOT EXISTS UsersBtreeIndex ( id INT UNIQUE, 
        birthDate DATE,  INDEX birthDate_btree (birthDate))`)
	if tableCreateError != nil {
		log.Fatal(tableCreateError)
	}

	noIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersNoIndex")
	noIndexEndDelete := time.Now()
	HashIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersHashIndex")
	HashIndexEndDelete := time.Now()
	BtreeIndexStartDelete := time.Now()
	deleteData(ctx, conn, "UsersBtreeIndex")
	BtreeIndexEndDelete := time.Now()

	stmtInsNoIndex := prepareStmt(ctx, conn, "UsersNoIndex")
	defer stmtInsNoIndex.Close()

	stmtInsHashIndex := prepareStmt(ctx, conn, "UsersHashIndex")
	defer stmtInsHashIndex.Close()

	stmtInsBtreeIndex := prepareStmt(ctx, conn, "UsersBtreeIndex")
	defer stmtInsBtreeIndex.Close()

	// Verify rows count

	noIndexStartInsert := time.Now()
	insertData(stmtInsNoIndex)
	noIndexEndInsert := time.Now()

	fmt.Println("Insert Execution time:")
	fmt.Println("No Index   : ", noIndexEndInsert.Sub(noIndexStartInsert).Seconds())

	// checkRowsCount(ctx, conn, "UsersNoIndex")

	HashIndexStartInsert := time.Now()
	insertData(stmtInsHashIndex)
	HashIndexEndInsert := time.Now()
	fmt.Println("Hash Index : ", HashIndexEndInsert.Sub(HashIndexStartInsert).Seconds())

	// checkRowsCount(ctx, conn, "UsersHashIndex")

	BtreeIndexStartInsert := time.Now()
	insertData(stmtInsBtreeIndex)
	BtreeIndexEndInsert := time.Now()
	fmt.Println("Btree Index: ", BtreeIndexEndInsert.Sub(BtreeIndexStartInsert).Seconds())

	// checkRowsCount(ctx, conn, "UsersBtreeIndex")

	fmt.Println("Delete Execution time:")
	fmt.Println("No Index   : ", noIndexEndDelete.Sub(noIndexStartDelete).Seconds())
	fmt.Println("Hash Index : ", HashIndexEndDelete.Sub(HashIndexStartDelete).Seconds())
	fmt.Println("Btree Index: ", BtreeIndexEndDelete.Sub(BtreeIndexStartDelete).Seconds())
}

func selectData(ctx context.Context, conn *sql.Conn) {
	noIndexStartSelect := time.Now()
	selectTableData(ctx, conn, "UsersNoIndex")
	noIndexEndSelect := time.Now()

	HashIndexStartSelect := time.Now()
	selectTableData(ctx, conn, "UsersHashIndex")
	HashIndexEndSelect := time.Now()

	BtreeIndexStartSelect := time.Now()
	selectTableData(ctx, conn, "UsersBtreeIndex")
	BtreeIndexEndSelect := time.Now()

	fmt.Println("Select Execution time:")
	fmt.Println("No Index   : ", noIndexEndSelect.Sub(noIndexStartSelect).Seconds())
	fmt.Println("Hash Index : ", HashIndexEndSelect.Sub(HashIndexStartSelect).Seconds())
	fmt.Println("Btree Index: ", BtreeIndexEndSelect.Sub(BtreeIndexStartSelect).Seconds())

}

func selectTableData(ctx context.Context, conn *sql.Conn, tableName string) {
	rows, err := conn.QueryContext(ctx,
		`SELECT birthDate FROM `+tableName+` WHERE birthDate>'0010-06-02'  
        ORDER BY birthDate DESC LIMIT 10000`)
	if err != nil {
		log.Fatal(err)
	}

	birthDates := make([]string, 1000)
	var birthDate string
	for rows.Next() {
		rows.Scan(&birthDate)
		birthDates = append(birthDates, birthDate)
	}

	fmt.Println(birthDates)
}

func main() {
	if len(os.Args[1:]) == 0 {
		fmt.Println("Please enter arguments: create|select")
		return
	}

	db, err := sql.Open("mysql", "root:pass@/mysql_flush_tx_1")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	conn, _ := db.Conn(ctx)

	if os.Args[1] == "create" {
		createData(ctx, conn)
	}

	if os.Args[1] == "select" {
		selectData(ctx, conn)
	}
}

// No Index   :  2128.2172231
// Hash Index :  2128.8227601
// Btree Index:  2090.8328692
//
//
