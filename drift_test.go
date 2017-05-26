package drift

import (
	"context"
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
	testTableA    = "testtableA"
	testTableB    = "testtableB"
)

func getTestDDBClient() *dynamodb.DynamoDB {
	creds := credentials.NewStaticCredentials("foo", "bar", "")
	sess := session.New(aws.NewConfig().WithRegion("us-west-2").WithMaxRetries(1).WithCredentials(creds))
	return dynamodb.New(sess, &aws.Config{Endpoint: aws.String("http://localhost:8000")})
}

func dropTestMetaTable(db *dynamodb.DynamoDB) {
	db.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(testMetaTable)})
}

func dropTestTables(db *dynamodb.DynamoDB) {
	db.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(testTableA)})
	db.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(testTableB)})
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

func setupTestTables(db *dynamodb.DynamoDB) error {
	cti := &dynamodb.CreateTableInput{
		TableName: aws.String(testTableA),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}
	_, err := db.CreateTable(cti)
	if err != nil {
		return fmt.Errorf("error creating test table A: %v", err)
	}
	cti.TableName = aws.String(testTableB)
	_, err = db.CreateTable(cti)
	if err != nil {
		return fmt.Errorf("error creating test table B: %v", err)
	}
	f, err := os.Open("testdata/table.json")
	if err != nil {
		return fmt.Errorf("error opening test table file: %v", err)
	}
	defer f.Close()
	data := []TestTableItem{}
	d := json.NewDecoder(f)
	err = d.Decode(&data)
	if err != nil {
		return fmt.Errorf("error unmarshaling test table data: %v", err)
	}
	for _, m := range data {
		item, err := dynamodbattribute.MarshalMap(m)
		if err != nil {
			return fmt.Errorf("error marshaling data: %v", err)
		}
		in := &dynamodb.PutItemInput{
			TableName: aws.String(testTableA),
			Item:      item,
		}
		_, err = db.PutItem(in)
		if err != nil {
			return fmt.Errorf("error inserting data: %v", err)
		}
	}
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
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestTables(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up test tables: %v", err)
	}
	defer dropTestTables(dd.DynamoDB)
	err = dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	migration := &DynamoDrifterMigration{
		TableName:   testTableA,
		Description: "split up names",
		Callback:    testMigrateUp,
	}
	_, errs := dd.runCallbacks(context.Background(), migration, 2, false)
	if len(errs) != 0 {
		t.Fatalf("error running callbacks: %v", errs)
	}
}

func TestExecuteActions(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestTables(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up test tables: %v", err)
	}
	defer dropTestTables(dd.DynamoDB)
	err = dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	migration := &DynamoDrifterMigration{
		TableName:   testTableA,
		Description: "split up names",
		Callback:    testMigrateUp,
	}
	da, errs := dd.runCallbacks(context.Background(), migration, 2, false)
	if len(errs) != 0 {
		t.Fatalf("error running callbacks: %v", errs)
	}
	errs = dd.executeActions(context.Background(), migration, da, 2)
	if len(errs) != 0 {
		t.Fatalf("error executing actions: %v", errs)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableA)
	if err != nil {
		t.Fatalf("error verifying migration in table A: %v", err)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableB)
	if err != nil {
		t.Fatalf("error verifying migration in table B: %v", err)
	}
}

func TestRunMigration(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestTables(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up test tables: %v", err)
	}
	defer dropTestTables(dd.DynamoDB)
	err = dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	migration := &DynamoDrifterMigration{
		TableName:   testTableA,
		Description: "split up names",
		Callback:    testMigrateUp,
	}
	errs := dd.Run(context.Background(), migration, 2, false)
	if len(errs) != 0 {
		t.Fatalf("errors running migration: %v", errs)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableA)
	if err != nil {
		t.Fatalf("error verifying migration in table A: %v", err)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableB)
	if err != nil {
		t.Fatalf("error verifying migration in table B: %v", err)
	}
}

func TestUndoMigration(t *testing.T) {
	dd := DynamoDrifter{
		MetaTableName: testMetaTable,
		DynamoDB:      getTestDDBClient(),
	}
	err := setupTestTables(dd.DynamoDB)
	if err != nil {
		t.Fatalf("error setting up test tables: %v", err)
	}
	defer dropTestTables(dd.DynamoDB)
	err = dd.Init(10, 10)
	if err != nil {
		t.Fatalf("error in Init: %v", err)
	}
	defer dropTestMetaTable(dd.DynamoDB)
	migration := &DynamoDrifterMigration{
		TableName:   testTableA,
		Description: "split up names",
		Callback:    testMigrateUp,
	}
	errs := dd.Run(context.Background(), migration, 2, false)
	if len(errs) != 0 {
		t.Fatalf("errors running migration: %v", errs)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableA)
	if err != nil {
		t.Fatalf("error verifying migration in table A: %v", err)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableB)
	if err != nil {
		t.Fatalf("error verifying migration in table B: %v", err)
	}
	undoMigration := &DynamoDrifterMigration{
		TableName:   testTableA,
		Description: "put names back together",
		Callback:    testMigrateDown,
	}
	errs = dd.Undo(context.Background(), undoMigration, 2, false)
	if len(errs) != 0 {
		t.Fatalf("errors running undo migration: %v", errs)
	}
	err = testVerifyMigration(dd.DynamoDB, testTableA)
	if err == nil {
		t.Fatalf("verification of table A should have failed")
	}
	err = testVerifyMigration(dd.DynamoDB, testTableB)
	if err == nil {
		t.Fatalf("verification of table B should have failed")
	}
}

func testVerifyMigration(db *dynamodb.DynamoDB, tn string) error {
	table := []TestTableItem{}
	out, err := db.Scan(&dynamodb.ScanInput{TableName: &tn})
	if err != nil {
		return fmt.Errorf("error scanning table: %v", err)
	}
	for _, item := range out.Items {
		tti := TestTableItem{}
		err = dynamodbattribute.UnmarshalMap(item, &tti)
		if err != nil {
			return fmt.Errorf("error unmarshaling item from table: %v", err)
		}
		if tti.FirstName == "" || tti.LastName == "" {
			return fmt.Errorf("missing data: %v", tti)
		}
		table = append(table, tti)
	}
	if len(table) != 3 {
		return fmt.Errorf("bad length for items: %v", len(table))
	}
	return nil
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

type TestUpdateOldDynamoItem struct {
	Name string `dynamodbav:":n"`
}

type TestUpdateNewDynamoItem struct {
	FirstName string `dynamodbav:":fn"`
	LastName  string `dynamodbav:":ln"`
}

type TestInsertDynamoItem struct {
	ID        int    `dynamodbav:"ID"`
	FirstName string `dynamodbav:"FirstName"`
	LastName  string `dynamodbav:"LastName"`
}

// Callbacks are executed once for each item in the target table
func testMigrateUp(item RawDynamoItem, action *DrifterAction) error {
	name := *item["Name"].S
	ns := strings.Split(name, " ")
	newitem := TestUpdateNewDynamoItem{
		FirstName: ns[0],
		LastName:  ns[1],
	}
	id, err := strconv.Atoi(*item["ID"].N)
	if err != nil {
		return fmt.Errorf("bad id: %v", err)
	}
	key := TestDynamoKey{
		ID: id,
	}
	insertitem := TestInsertDynamoItem{
		ID:        id,
		FirstName: ns[0],
		LastName:  ns[1],
	}
	err = action.Insert(insertitem, testTableB)
	if err != nil {
		return fmt.Errorf("error inserting item action: %v", err)
	}
	return action.Update(key, newitem, "SET FirstName = :fn, LastName = :ln", nil, "")
}

func testMigrateDown(item RawDynamoItem, action *DrifterAction) error {
	olditem := TestUpdateOldDynamoItem{
		Name: *item["FirstName"].S + *item["LastName"].S,
	}
	id, err := strconv.Atoi(*item["ID"].N)
	if err != nil {
		return fmt.Errorf("bad id: %v", err)
	}
	key := TestDynamoKey{
		ID: id,
	}
	err = action.Delete(key, testTableB)
	if err != nil {
		return fmt.Errorf("error deleting item action: %v", err)
	}

	return action.Update(key, olditem, "SET #n = :n REMOVE FirstName, LastName", map[string]string{"#n": "Name"}, "")
}
