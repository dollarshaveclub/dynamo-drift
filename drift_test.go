package drift

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// These tests all require DynamoDBLocal running on localhost:8000
// https://aws.amazon.com/blogs/aws/dynamodb-local-for-desktop-development/

const (
	testMetaTable = "metatable"
	testTable     = "testtable"
)

func getTestDDBClient() *dynamodb.DynamoDB {
	creds := credentials.NewStaticCredentials("foo", "bar", "")
	sess := session.New(aws.NewConfig().WithRegion("us-west-2").WithMaxRetries(1).WithCredentials(creds))
	return dynamodb.New(sess, &aws.Config{Endpoint: aws.String("http://localhost:8000")})
}

func dropTestMetaTable(db *dynamodb.DynamoDB) {
	_, _ = db.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(testMetaTable)})
}

func setupTestMetaTable(db *dynamodb.DynamoDB) error {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      db,
	}
	err := dd.createMetaTable(10, 10, testMetaTable)
	if err != nil {
		return err
	}
	f, err := os.Open("testdata/migrations.json")
	if err != nil {
		return fmt.Errorf("error opening test migrations file: %v", err)
	}
	defer f.Close()
	data := []DynamoDrifterMigration{}
	d := json.NewDecoder(f)
	err = d.Decode(&data)
	if err != nil {
		return fmt.Errorf("error unmarshaling test data: %v", err)
	}
	for _, m := range data {
		item, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			return fmt.Errorf("error marshaling data: %v", err)
		}
		in := &dynamodb.PutItemInput{
			TableName: aws.String(testMetaTable),
			Item:      item,
		}
		_, err = db.PutItem(in)
		if err != nil {
			return fmt.Errorf("error inserting data: %v", err)
		}
	}
	return nil
}

func setupTestTable(db *dynamodb.DynamoDB) error {
	return nil
}

func TestCreateMetaTable(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := dd.createMetaTable(10, 10, dd.MetaTableName)
	if err != nil {
		t.Fatalf("error creating metatable: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	ok, err := dd.findTable(dd.MetaTableName)
	if err != nil {
		t.Fatalf("error finding metatable: %v", err)
	}
	if !ok {
		t.Fatalf("metatable not found")
	}
}

func TestInit(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	ok, err := dd.findTable(dd.MetaTableName)
	if err != nil {
		t.Fatalf("error finding metatable: %v", err)
	}
	if !ok {
		t.Fatalf("metatable not found")
	}
	err = dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error running init x2: %v", err)
	}
	resp, err := dd.DynamoDB.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		t.Fatalf("error listing tables: %v", err)
	}
	if len(resp.TableNames) != 1 {
		t.Fatalf("unexpected tables: %v", resp.TableNames)
	}
}

func TestApplied(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestMetaTable(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up metatable: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	ml, err := dd.Applied()
	if err != nil {
		t.Fatalf("error in Applied: %v", err)
	}
	if len(ml) != 3 {
		t.Fatalf("unexpected migrations count: %v", len(ml))
	}
	if ml[0].Number != 0 || ml[2].Number != 2 {
		t.Fatalf("bad sort order: %v", ml)
	}
}

func TestInsertMetaItem(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	m := &DynamoDrifterMigration{Number: 0}
	err = dd.insertMetaItem(m)
	if err != nil {
		t.Fatalf("error inserting meta item: %v", err)
	}
	ml, err := dd.Applied()
	if err != nil {
		t.Fatalf("error in Applied: %v", err)
	}
	if len(ml) != 1 {
		t.Fatalf("unexpected migrations count: %v", len(ml))
	}
	if ml[0].Number != 0 {
		t.Fatalf("bad migration number: %v", ml[0].Number)
	}
}

func TestDeleteMetaItem(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestMetaTable(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up metatable: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	ml, err := dd.Applied()
	if err != nil {
		t.Fatalf("error in Applied: %v", err)
	}
	if len(ml) != 3 {
		t.Fatalf("unexpected migrations count: %v", len(ml))
	}
	m := &DynamoDrifterMigration{Number: 2}
	err = dd.deleteMetaItem(m)
	if err != nil {
		t.Fatalf("error deleting meta item: %v", err)
	}
	ml, err = dd.Applied()
	if err != nil {
		t.Fatalf("error in Applied: %v", err)
	}
	if len(ml) != 2 {
		t.Fatalf("unexpected migrations count: %v", len(ml))
	}
}

func TestRunCallbacks(t *testing.T) {
}

func TestExecuteActions(t *testing.T) {

}

type TestTableItem struct {
	ID        int    `dynamodbav:"ID" json:"id"`
	Name      string `dynamodbav:"Name" json:"name"`
	FirstName string `dynamodbav:"FirstName" json:"first_name"`
	LastName  string `dynamodbav:"LastName" json:"last_name"`
}

type TestDynamoKey struct {
	ID int `dynamodbav:"ID"`
}

type TestOldDynamoItem struct {
	Name string `dynamodbav:":n"`
}

type TestNewDynamoItem struct {
	FirstName string `dynamodbav:":fn"`
	LastName  string `dynamodbav:":ln"`
}

// Callbacks are executed once for each item in the target table
func testMigrateUp(item RawDynamoItem, action *DrifterAction) error {
	name := item["Name"].String()
	ns := strings.Split(name, " ")
	newitem := TestNewDynamoItem{
		FirstName: ns[0],
		LastName:  ns[1],
	}
	id, err := strconv.Atoi(item["ID"].String())
	if err != nil {
		return fmt.Errorf("bad id: %v", err)
	}
	key := TestDynamoKey{
		ID: id,
	}
	return action.Update(key, newitem, "SET FirstName = :fn, LastName = :ln", nil, "")
}

func testMmigrateDown(item RawDynamoItem, action *DrifterAction) error {
	olditem := TestOldDynamoItem{
		Name: item["FirstName"].String() + item["LastName"].String(),
	}
	id, err := strconv.Atoi(item["ID"].String())
	if err != nil {
		return fmt.Errorf("bad id: %v", err)
	}
	key := TestDynamoKey{
		ID: id,
	}
	return action.Update(key, olditem, "SET Name = :n REMOVE FirstName, LastName", nil, "")
}
