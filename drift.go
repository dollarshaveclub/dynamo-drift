package drift

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/dollarshaveclub/jobmanager"
)

// RawDynamoItem models an item from DynamoDB as returned by the API
type RawDynamoItem map[string]*dynamodb.AttributeValue

// DynamoMigrationFunction is a callback run for each item in the DynamoDB table
// item is the raw item
// action is the DrifterAction object used to mutate/add/remove items
type DynamoMigrationFunction func(item RawDynamoItem, action *DrifterAction) error

// DynamoDrifterMigration models an individual migration
type DynamoDrifterMigration struct {
	Number      uint                    `dynamodbav:"Number" json:"number"`           // Monotonic number of the migration (ascending)
	TableName   string                  `dynamodbav:"TableName" json:"tablename"`     // DynamoDB table the migration applies to
	Description string                  `dynamodbav:"Description" json:"description"` // Free-form description of what the migration does
	Callback    DynamoMigrationFunction `dynamodbav:"-" json:"-"`                     // Callback for each item in the table
}

// DynamoDrifter is the object that manages and performs migrations
type DynamoDrifter struct {
	MetaTableName string             // Table to store migration tracking metadata
	DynamoDB      *dynamodb.DynamoDB // Fully initialized and authenticated DynamoDB client
	q             actionQueue
}

func (dd *DynamoDrifter) createMetaTable(pwrite, pread uint, metatable string) error {
	cti := &dynamodb.CreateTableInput{
		TableName: aws.String(metatable),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String("Number"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				AttributeName: aws.String("Number"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(int64(pread)),
			WriteCapacityUnits: aws.Int64(int64(pwrite)),
		},
	}
	_, err := dd.DynamoDB.CreateTable(cti)
	return err
}

func (dd *DynamoDrifter) findTable(table string) (bool, error) {
	var err error
	var lto *dynamodb.ListTablesOutput
	lti := &dynamodb.ListTablesInput{}
	for {
		lto, err = dd.DynamoDB.ListTables(lti)
		if err != nil {
			return false, fmt.Errorf("error listing tables: %v", err)
		}
		for _, tn := range lto.TableNames {
			if tn != nil && *tn == table {
				return true, nil
			}
		}
		if lto.LastEvaluatedTableName == nil {
			return false, nil
		}
		lti.ExclusiveStartTableName = lto.LastEvaluatedTableName
	}
}

// Init creates the metadata table if necessary. It is safe to run Init multiple times (it's a noop if metadata table already exists).
// pread and pwrite are the provisioned read and write values to use with table creation, if necessary
func (dd *DynamoDrifter) Init(pwrite, pread uint) error {
	if dd.DynamoDB == nil {
		return fmt.Errorf("DynamoDB client is required")
	}
	extant, err := dd.findTable(dd.MetaTableName)
	if err != nil {
		return fmt.Errorf("error checking if meta table exists: %v", err)
	}
	if !extant {
		err = dd.createMetaTable(pwrite, pread, dd.MetaTableName)
		if err != nil {
			return fmt.Errorf("error creating meta table: %v", err)
		}
	}
	return nil
}

// Applied returns all applied migrations as tracked in metadata table in ascending order
func (dd *DynamoDrifter) Applied() ([]DynamoDrifterMigration, error) {
	if dd.DynamoDB == nil {
		return nil, fmt.Errorf("DynamoDB client is required")
	}
	in := &dynamodb.ScanInput{
		TableName: &dd.MetaTableName,
	}
	ms := []DynamoDrifterMigration{}
	var consumeErr error
	consumePage := func(resp *dynamodb.ScanOutput, last bool) bool {
		for _, v := range resp.Items {
			m := DynamoDrifterMigration{}
			consumeErr = dynamodbattribute.UnmarshalMap(v, &m)
			if consumeErr != nil {
				return false // stop paging
			}
			ms = append(ms, m)
		}
		return true
	}

	err := dd.DynamoDB.ScanPages(in, consumePage)
	if err != nil {
		return nil, err
	}
	if consumeErr != nil {
		return nil, consumeErr
	}

	// sort by Number
	sort.Slice(ms, func(i, j int) bool { return ms[i].Number < ms[j].Number })

	return ms, nil
}

func (dd *DynamoDrifter) doCallback(ctx context.Context, params ...interface{}) error {
	if len(params) != 3 {
		return fmt.Errorf("bad parameter count: %v (want 3)", len(params))
	}
	callback, ok := params[0].(DynamoMigrationFunction)
	if !ok {
		return fmt.Errorf("bad type for DynamoMigrationFunction: %T", params[0])
	}
	item, ok := params[1].(map[string]*dynamodb.AttributeValue)
	if !ok {
		return fmt.Errorf("bad type for RawDynamoItem: %T", params[1])
	}
	da, ok := params[2].(*DrifterAction)
	if !ok {
		return fmt.Errorf("bad type for *DrifterAction: %T", params[2])
	}
	return callback(item, da)
}

type errorCollector struct {
	sync.Mutex
	errs []error
}

func (ec *errorCollector) clear() {
	ec.Lock()
	ec.errs = []error{}
	ec.Unlock()
}

func (ec *errorCollector) HandleError(err error) error {
	ec.Lock()
	ec.errs = append(ec.errs, err)
	ec.Unlock()
	return nil
}

func (dd *DynamoDrifter) progressMsg(cp, ae uint, cerrs, aerrs []error, progressChan chan *MigrationProgress) {
	if progressChan != nil {
		select {
		case progressChan <- &MigrationProgress{
			CallbacksProcessed: cp,
			ActionsExecuted:    ae,
			CallbackErrors:     cerrs,
			ActionErrors:       aerrs,
		}:
			return
		default:
			return
		}
	}
}

// runCallbacks gets items from the target table in batches of size concurrency, populates a JobManager with them and then executes all jobs in parallel
func (dd *DynamoDrifter) runCallbacks(ctx context.Context, migration *DynamoDrifterMigration, concurrency uint, failOnFirstError bool, progressChan chan *MigrationProgress) (*DrifterAction, []error) {
	errs := []error{}
	ec := errorCollector{}
	da := &DrifterAction{}
	jm := jobmanager.New()
	jm.ErrorHandler = &ec
	jm.Concurrency = concurrency
	jm.Identifier = "migration-callbacks"

	si := &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		TableName:      &migration.TableName,
		Limit:          aws.Int64(int64(concurrency) * 100),
	}
	var cp uint
	for {
		so, err := dd.DynamoDB.Scan(si)
		if err != nil {
			return nil, []error{fmt.Errorf("error scanning migration table: %v", err)}
		}
		j := &jobmanager.Job{
			Job: dd.doCallback,
		}
		for _, item := range so.Items {
			jm.AddJob(j, migration.Callback, item, da)
		}
		jm.Run(ctx)
		if len(ec.errs) != 0 && failOnFirstError {
			return nil, ec.errs
		}
		cp += uint(len(so.Items))
		dd.progressMsg(cp, 0, ec.errs, nil, progressChan)
		errs = append(errs, ec.errs...)
		ec.clear()
		if so.LastEvaluatedKey == nil {
			return da, errs
		}
		si.ExclusiveStartKey = so.LastEvaluatedKey
	}
}

func (dd *DynamoDrifter) doAction(ctx context.Context, params ...interface{}) error {
	if len(params) != 2 {
		return fmt.Errorf("bad parameter length: %v (want 2)", len(params))
	}
	action, ok := params[0].(*action)
	if !ok {
		return fmt.Errorf("bad type for *action: %T", params[0])
	}
	tn, ok := params[1].(string)
	if !ok {
		return fmt.Errorf("bad type for tablename: %T", params[1])
	}
	if action.tableName != "" {
		tn = action.tableName
	}
	switch action.atype {
	case updateAction:
		uii := &dynamodb.UpdateItemInput{
			TableName:                 &tn,
			Key:                       action.keys,
			UpdateExpression:          aws.String(action.updExpr),
			ExpressionAttributeValues: action.values,
			ExpressionAttributeNames:  action.expAttrNames,
		}
		_, err := dd.DynamoDB.UpdateItem(uii)
		if err != nil {
			return fmt.Errorf("error updating item: %v", err)
		}
		return nil
	case insertAction:
		pii := &dynamodb.PutItemInput{
			TableName: &tn,
			Item:      action.item,
		}
		_, err := dd.DynamoDB.PutItem(pii)
		if err != nil {
			return fmt.Errorf("error inserting item: %v", err)
		}
		return nil
	case deleteAction:
		dii := &dynamodb.DeleteItemInput{
			TableName: &tn,
			Key:       action.keys,
		}
		_, err := dd.DynamoDB.DeleteItem(dii)
		if err != nil {
			return fmt.Errorf("error deleting item: %v", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown action type: %v", action.atype)
	}
}

func (dd *DynamoDrifter) executeActions(ctx context.Context, migration *DynamoDrifterMigration, da *DrifterAction, concurrency uint, failonFirstError bool, progressChan chan *MigrationProgress) []error {
	ec := errorCollector{}
	var jm *jobmanager.JobManager
	getnewjm := func() { // you can only Run() a JobManager once
		jm = jobmanager.New()
		jm.ErrorHandler = &ec
		jm.Concurrency = concurrency
		jm.Identifier = "migration-actions"
	}
	getnewjm()
	var i int
	for i = range da.aq.q {
		j := &jobmanager.Job{
			Job: dd.doAction,
		}
		jm.AddJob(j, &(da.aq.q[i]), migration.TableName)
		if i != 0 && i%10 == 0 {
			jm.Run(ctx)
			if len(ec.errs) != 0 && failonFirstError {
				return ec.errs
			}
			getnewjm()
			dd.progressMsg(0, uint(i+1), nil, ec.errs, progressChan)
		} else {
		}
	}
	if i != 0 {
		jm.Run(ctx)
		dd.progressMsg(0, uint(i), nil, ec.errs, progressChan)
	}
	return ec.errs
}

func (dd *DynamoDrifter) insertMetaItem(m *DynamoDrifterMigration) error {
	mi, err := dynamodbattribute.MarshalMap(m)
	if err != nil {
		return fmt.Errorf("error marshaling migration: %v", err)
	}
	pi := &dynamodb.PutItemInput{
		TableName: &dd.MetaTableName,
		Item:      mi,
	}
	_, err = dd.DynamoDB.PutItem(pi)
	if err != nil {
		return fmt.Errorf("error inserting migration item into meta table: %v", err)
	}
	return nil
}

func (dd *DynamoDrifter) deleteMetaItem(m *DynamoDrifterMigration) error {
	di := &dynamodb.DeleteItemInput{
		TableName: &dd.MetaTableName,
		Key: map[string]*dynamodb.AttributeValue{
			"Number": &dynamodb.AttributeValue{
				N: aws.String(strconv.Itoa(int(m.Number))),
			},
		},
	}
	_, err := dd.DynamoDB.DeleteItem(di)
	if err != nil {
		return fmt.Errorf("error deleting item from meta table: %v", err)
	}
	return nil
}

func (dd *DynamoDrifter) run(ctx context.Context, migration *DynamoDrifterMigration, concurrency uint, failOnFirstError bool, progressChan chan *MigrationProgress) []error {
	if migration == nil || migration.Callback == nil {
		return []error{fmt.Errorf("migration is required")}
	}
	if concurrency == 0 {
		concurrency = 1
	}
	if migration.TableName == "" {
		return []error{fmt.Errorf("TableName is required")}
	}
	extant, err := dd.findTable(migration.TableName)
	if err != nil {
		return []error{fmt.Errorf("error finding migration table: %v", err)}
	}
	if !extant {
		return []error{fmt.Errorf("table %v not found", migration.TableName)}
	}
	da, errs := dd.runCallbacks(ctx, migration, concurrency, failOnFirstError, progressChan)
	if len(errs) != 0 {
		return errs
	}
	errs = dd.executeActions(ctx, migration, da, concurrency, failOnFirstError, progressChan)
	if len(errs) != 0 {
		return errs
	}
	return []error{}
}

// MigrationProgress models periodic progress information communicated back to the caller
type MigrationProgress struct {
	CallbacksProcessed uint
	ActionsExecuted    uint
	CallbackErrors     []error
	ActionErrors       []error
}

// Run runs an individual migration at the specified concurrency and blocks until finished.
// concurrency controls the number of table items processed concurrently (value of one will guarantee order of migration actions).
// failOnFirstError causes Run to abort on first error, otherwise the errors will be queued and reported only after all items have been processed.
// progressChan is an optional channel on which periodic MigrationProgress messages will be sent
func (dd *DynamoDrifter) Run(ctx context.Context, migration *DynamoDrifterMigration, concurrency uint, failOnFirstError bool, progressChan chan *MigrationProgress) []error {
	if dd.DynamoDB == nil {
		return []error{fmt.Errorf("DynamoDB client is required")}
	}
	if progressChan != nil {
		defer close(progressChan)
	}
	errs := dd.run(ctx, migration, concurrency, failOnFirstError, progressChan)
	if len(errs) != 0 {
		return errs
	}
	err := dd.insertMetaItem(migration)
	if err != nil {
		return []error{err}
	}
	return []error{}
}

// Undo "undoes" a migration by running the supplied migration but deletes the corresponding metadata record if successful
func (dd *DynamoDrifter) Undo(ctx context.Context, undoMigration *DynamoDrifterMigration, concurrency uint, failOnFirstError bool, progressChan chan *MigrationProgress) []error {
	if dd.DynamoDB == nil {
		return []error{fmt.Errorf("DynamoDB client is required")}
	}
	errs := dd.run(ctx, undoMigration, concurrency, failOnFirstError, progressChan)
	if len(errs) != 0 {
		return errs
	}
	err := dd.deleteMetaItem(undoMigration)
	if err != nil {
		return []error{err}
	}
	return []error{}
}

type actionType int

const (
	updateAction actionType = iota
	insertAction
	deleteAction
)

type action struct {
	atype        actionType
	keys         RawDynamoItem
	values       RawDynamoItem
	item         RawDynamoItem
	updExpr      string
	expAttrNames map[string]*string
	tableName    string
}

type actionQueue struct {
	sync.Mutex
	q []action
}

// DrifterAction is an object useful for performing actions within the migration callback. All actions performed by methods on DrifterAction are queued and performed *after* all existing items have been iterated over and callbacks performed.
// DrifterAction can be used in multiple goroutines by the callback, but must not be retained after the callback returns.
// If concurrency > 1, order of queued operations cannot be guaranteed.
type DrifterAction struct {
	dyn *dynamodb.DynamoDB
	aq  actionQueue
}

// Update mutates the given keys using fields and updateExpression.
// keys and values are arbitrary structs with "dynamodbav" annotations. IMPORTANT: annotation names must match the names used in updateExpression.
// updateExpression is the native DynamoDB update expression. Ex: "SET foo = :bar" (in this example keys must have a field annotated "foo" and values must have a field annotated ":bar").
// expressionAttributeNames is optional but used if item attribute names are reserved keywords. For example: "SET #n = :name", expressionAttributeNames: map[string]string{"#n":"Name"}.
//
// Required: keys, values, updateExpression
//
// Optional: expressionAttributeNames, tableName (defaults to migration table)
func (da *DrifterAction) Update(keys interface{}, values interface{}, updateExpression string, expressionAttributeNames map[string]string, tableName string) error {
	mkeys, err := dynamodbattribute.MarshalMap(keys)
	if err != nil {
		return fmt.Errorf("error marshaling keys: %v", err)
	}
	mvals, err := dynamodbattribute.MarshalMap(values)
	if err != nil {
		return fmt.Errorf("error marshaling values: %v", err)
	}
	if updateExpression == "" {
		return fmt.Errorf("updateExpression is required")
	}
	var ean map[string]*string
	if expressionAttributeNames != nil && len(expressionAttributeNames) > 0 {
		ean = map[string]*string{}
		for k, v := range expressionAttributeNames {
			ean[k] = &v
		}
	}
	ua := action{
		atype:        updateAction,
		keys:         mkeys,
		values:       mvals,
		updExpr:      updateExpression,
		expAttrNames: ean,
		tableName:    tableName,
	}
	da.aq.Lock()
	da.aq.q = append(da.aq.q, ua)
	da.aq.Unlock()
	return nil
}

// Insert inserts item into the specified table.
// item is an arbitrary struct with "dynamodbav" annotations.
// tableName is optional (defaults to migration table).
func (da *DrifterAction) Insert(item interface{}, tableName string) error {
	mitem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("error marshaling item: %v", err)
	}
	ia := action{
		atype:     insertAction,
		item:      mitem,
		tableName: tableName,
	}
	da.aq.Lock()
	da.aq.q = append(da.aq.q, ia)
	da.aq.Unlock()
	return nil
}

// Delete deletes the specified item(s).
// keys is an arbitrary struct with "dynamodbav" annotations.
// tableName is optional (defaults to migration table).
func (da *DrifterAction) Delete(keys interface{}, tableName string) error {
	mkeys, err := dynamodbattribute.MarshalMap(keys)
	if err != nil {
		return fmt.Errorf("error marshaling keys: %v", err)
	}
	dla := action{
		atype:     deleteAction,
		keys:      mkeys,
		tableName: tableName,
	}
	da.aq.Lock()
	da.aq.q = append(da.aq.q, dla)
	da.aq.Unlock()
	return nil
}

// DynamoDB returns the DynamoDB client object
func (da *DrifterAction) DynamoDB() *dynamodb.DynamoDB {
	return da.dyn
}
