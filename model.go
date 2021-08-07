package sqlmodelgen

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"

	"github.com/skillian/logging"

	"github.com/skillian/expr/errors"
	"github.com/skillian/expr/internal"
	"github.com/skillian/expr/stream/sqlstream"
	"github.com/skillian/expr/stream/sqlstream/sqlmodelgen/config"
	"github.com/skillian/expr/stream/sqlstream/sqltypes"
)

var logger = logging.GetLogger("sqlmodelgen")

// CommonData embedded in every model type
type CommonData struct {
	// Doc is documentation for the model that can be included
	// in the generated code
	Doc string
}

// Config is a root configuration that can span multiple databases
// (but that might not be necessary, depending on your project).
type Config struct {
	Namespace       string
	Namespaces      []string
	Databases       []*Database
	DatabasesByName map[string]*Database
	DatabaseNamers  Namers
}

// ConfigFromJSON reads JSON data from the reader, r, and
// instantiates a Config model from it.
func ConfigFromJSON(r io.Reader, mc ModelContext) (*Config, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Errorf1From(
			err, "failed to load JSON from %v", r)
	}
	var j config.Config
	if err = json.Unmarshal(data, &j); err != nil {
		return nil, errors.Errorf1From(
			err, "failed to parse %q as JSON", data)
	}
	c := &Config{}
	if err = (&configBuilder{Config: c, ModelContext: mc}).init(&j); err != nil {
		js, err2 := json.MarshalIndent(j, "", "\t")
		if err2 != nil {
			js = []byte("(error!)")
			err = errors.Aggregate(
				errors.Errorf1From(
					err2, "failed to marshal %v into JSON",
					data,
				),
				err,
			)
		}
		return nil, errors.Errorf1From(
			err, "failed to initialize configuration "+
				"from JSON:\n\n%v", js)
	}
	return c, nil
}

func (c *Config) MarshalJSON() (Bs []byte, Err error) {
	textOf := func(v interface{}) (string, error) {
		if m, ok := v.(encoding.TextMarshaler); ok {
			bs, err := m.MarshalText()
			if err != nil {
				return "", err
			}
			return string(bs), nil
		}
		return fmt.Sprint(v), nil
	}
	namerNameOf := func(n sqlstream.Namer, what string, args ...interface{}) (string, error) {
		name := textOf(n)
		if err != nil {
			what = fmt.Sprintf(what, args...)
			return "", errors.Errorf2From(
				err, "failed to marshal %[1]s namer: "+
					"%[2]v (type: %[2]T)",
				what, n,
			)
		}
		return name, nil
	}
	type srcTrgNamer struct {
		source sqlstream.Namer
		target *string
		name string
	}
	initNamers := func(stns []srcTrgNamer) (err error) {
		for _, stn := range stns {
			if *stn.target, err = namerNameOf(
				stn.source,
				stn.name,
			); err != nil {
				return
			}
		}
		return nil
	}
	var err error
	j := config.Config{
		Namespace: c.Namespace,
		Databases: make(map[string]config.Database, len(c.Databases)),
		DatabaseNamers: config.Namers{},
	}
	stns := []srcTrgNamer{
		{
			c.DatabaseNamers.SQLNamer,
			&j.DatabaseNamers.SQLNamer,
			"DatabaseNamers.SQLNamer",
		},
		{
			c.DatabaseNamers.ModelNamer,
			&j.DatabaseNamers.ModelNamer,
			"DatabaseNamers.ModelNamer",
		},
	}
	if err = initNamers(stns); err != nil {
		return nil, err
	}
	for _, db := range c.Databases {
		jdb := config.Database{
			Schemas: make(map[string]config.Schema, len(db.Schemas),
		}
		stns = append(stns[:0],
			srcTrgNamer{
				db.Namers.Table,
				&jdb.Namers.Table,
				"%s.Namers.Table",
			},
			srcTrgNamer{
				db.Namers.IDType,
				&jdb.Namers.IDType,
				"%s.Namers.IDType",
			},
			srcTrgNamer{
				db.Namers.KeyType,
				&jdb.Namers.KeyType,
				"%s.Namers.KeyType",
			},
			srcTrgNamer{
				db.Namers.Column,
				&jdb.Namers.Column,
				"%s.Namers.Column",
			},
			srcTrgNamer{
				db.Namers.Schema,
				&jdb.Namers.Schema,
				"%s.Namers.Schema",
			},
		)
		if err = initNamers(stns); err != nil {
			return nil, err
		}
		for _, sch := range db.Schemas {
			jsch := config.Schema{
				Tables: make(map[string]config.Table, len(sch.Tables)),
				Views: make(map[string]config.View, len(sch.Views)),
			}
			for _, tbl := range sch.Tables {
				jtbl := config.Table{
					Columns: make(map[string]config.Column, len(tbl.Columns)),
				}
				for _, col := range tbl.Columns {
					jcol := config.Column{
						PK: col.PK,
						FK: RawPathToFK(col)
						Type: col.Type.String(),
					}
					jtbl.Columns[col.RawName] = jcol
				}
				jsch.Tables[tbl.RawName] = jtbl
			}
			for _, vw := range sch.Views {
				jvw := config.View{
					Columns: make(map[string]config.Column, len(tbl.Columns)),
				}
				for _, col := range tbl.Columns {
					jcol := config.Column{
						PK: col.PK,
						FK: RawPathToFK(col)
						Type: col.Type.String(),
					}
					jvw.Columns[col.RawName] = jcol
				}
				jsch.Views[vw.RawName] = jvw
			}
			jdb.Schemas[sch.RawName] = jsch
		}
		j.Databases[db.RawName] = jdb
	}
	return json.Marshal(j)
}

type Namers struct {
	SQLNamer   sqlstream.Namer
	ModelNamer sqlstream.Namer
}

// TODO: Change Namer to accept a context

func (nrs *Namers) init(c *config.Namers) error {
	nr, err := sqlstream.NamerFromName(c.SQLNamer)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to initialize SQL namer: %q",
			c.SQLNamer,
		)
	}
	nrs.SQLNamer = nr
	nrs.ModelNamer, err = sqlstream.NamerFromName(c.ModelNamer)
	if err != nil {
		return errors.Errorf1From(
			err, "failed to initialize model namer: %q",
			c.ModelNamer,
		)
	}
	return nil
}

type Names struct {
	// RawName is the name as it appears in the configuration, with
	// spaces and casing ignored.  The SQLName and ModelName fields
	// are each generated with a sqlmodel.Namer.
	RawName string

	// SQLName is the name of the object as it appears in the
	// database
	SQLName string

	// ModelName is the name of the object as it appears in the
	// model (e.g. generated source code).
	ModelName string
}

func (ns *Names) init(rawName string, nrs *Namers) {
	ns.RawName = rawName
	ns.SQLName = nrs.SQLNamer.Apply(rawName)
	ns.ModelName = nrs.ModelNamer.Apply(rawName)
}

type Database struct {
	Config *Config
	Names
	Schemas       []*Schema
	SchemasByName map[string]*Schema

	IDs  []*Column
	Keys []*TableKey

	Namers struct {
		Column   Namers
		ID       Namers
		Key      Namers
		Table    Namers
		Schema   Namers
		Database Namers
	}
}

type Schema struct {
	Database *Database
	Names
	Tables       []*Table
	TablesByName map[string]*Table
	//Views    []*View
}

type Table struct {
	*Schema
	Names
	Columns       []*Column
	ColumnsByName map[string]*Column

	// PK is non-nil if the table has a single scalar primary key.
	// If PK is not nil, Key is nil.
	PK *TableID

	// Key is non-nil if the table has a composite key.
	// If Key is not nil, PK is nil.
	Key *TableKey

	// DataColumns are any non-ID and non-Key columns.  Referencing
	// these columns from here instead of checking if columns are PK
	// or Keys results in less logic in the templates.
	DataColumns []*Column
}

type TableID struct {
	Names
	Column *Column
}

type TableKey struct {
	Names
	IDs []*TableID
}

type Column struct {
	Table *Table
	Names
	Type sqltypes.Type
	PK   bool
	FK   *TableID
}

type View Table

type configBuilder struct {
	*Config
	ModelContext
	namespaces map[string]struct{}
	caches     struct {
		columns   []Column
		tables    []Table
		schemas   []Schema
		databases []Database
		ids       []TableID
		keys      []TableKey
		keyIDs    []*TableID
	}
}

func (b *configBuilder) init(c *config.Config) (err error) {
	b.namespaces = make(map[string]struct{}, 8)
	tempIDs := make([]*TableID, 0, 16)
	if err = b.Config.DatabaseNamers.init(&c.DatabaseNamers); err != nil {
		return
	}
	b.Config.Namespace = c.Namespace
	b.Config.Databases = make([]*Database, 0, len(c.Databases))
	b.Config.DatabasesByName = make(map[string]*Database, len(c.Databases))
	for dbName, dbCfg := range c.Databases {
		d, dbErr := b.newDatabase(dbName, &dbCfg)
		if dbErr != nil {
			return errors.Errorf1From(
				dbErr, "failed to initialize "+
					"database: %q", dbName)
		}
		b.Databases = append(b.Databases, d)
		b.DatabasesByName[dbName] = d
		for schName, schCfg := range dbCfg.Schemas {
			s := b.newSchema(d, schName, &schCfg)
			d.Schemas = append(d.Schemas, s)
			d.SchemasByName[schName] = s
			for tblName, tblCfg := range schCfg.Tables {
				t := b.newTable(s, tblName, &tblCfg)
				s.Tables = append(s.Tables, t)
				s.TablesByName[tblName] = t
				for colName, colCfg := range tblCfg.Columns {
					c := b.newColumn(t, colName, &colCfg)
					t.Columns = append(t.Columns, c)
					t.ColumnsByName[colName] = c
					c.PK = colCfg.PK
					if colCfg.Type != "" {
						c.Type, err = sqltypes.Parse(colCfg.Type)
						if err != nil {
							return errors.ErrorfFrom(
								err,
								"column %s.%s.%s.%s has an invalid Type",
								dbName, schName, tblName, colName,
							)
						}
						ns, _, err := b.ModelType(c.Type)
						if err != nil {
							return errors.ErrorfFrom(
								err,
								"failed to determine "+
									"model type of "+
									"column "+
									"%s.%s.%s.%s",
								dbName, schName,
								tblName, colName,
							)
						}
						if len(ns) > 0 {
							b.namespaces[ns] = struct{}{}
						}
					}
					if c.PK {
						id := b.newID(c, &colCfg)
						tempIDs = append(tempIDs, id)
					}
				}
				if len(tempIDs) > 0 {
					switch len(tempIDs) {
					case 1:
						t.PK = tempIDs[0]
					default:
						t.Key = b.newKey(t, tempIDs)
					}
					tempIDs = tempIDs[:0]
				}
			}
		}
	}
	if err = b.iterDBSchemaTableColumn(c, func(x dbSchemaTableColumn) error {
		if x.colCfg.FK == "" {
			return nil
		}
		fkTrg, err := b.getPathUp(x.colCfg.FK, x.table)
		if err != nil {
			return errors.ErrorfFrom(
				err, "failed to initialize column %v.%v.%v.%v FK",
				x.dbName, x.schName, x.tblName, x.colName,
			)
		}
		fkCol, ok := fkTrg.(*Column)
		if !ok {
			return errors.Errorf(
				"column %v.%v.%v.%v FK target is not a column",
				x.dbName, x.schName, x.tblName, x.colName,
			)
		}
		fkTbl := fkCol.Table
		if fkTbl.PK != nil && fkTbl.PK.Column == fkCol {
			x.column.FK = fkTbl.PK
			if x.column.Type == nil {
				x.column.Type = fkTbl.PK.Column.Type
			}
		} else if fkTbl.Key != nil {
			for _, id := range fkTbl.Key.IDs {
				if id.Column == fkCol {
					x.column.FK = id
					if x.column.Type == nil {
						x.column.Type = id.Column.Type
					}
					break
				}
			}
		}
		if x.column.FK == nil {
			return errors.Errorf(
				"column %q is not key within primary table %q",
				fkCol.RawName, fkCol.Table.RawName)
		}
		return nil
	}); err != nil {
		return err
	}
	if err = b.iterDBSchemaTableColumn(c, func(x dbSchemaTableColumn) error {
		if x.column.PK {
			return nil
		}
		if x.column.FK != nil {
			return nil
		}
		if x.table.Key != nil {
			for _, id := range x.table.Key.IDs {
				if id.Column == x.column {
					return nil
				}
			}
		}
		x.table.DataColumns = append(x.table.DataColumns, x.column)
		return nil
	}); err != nil {
		return err
	}
	if ens, ok := b.ModelContext.(NamespaceEnsurer); ok {
		for _, ns := range ens.EnsureNamespaces(b.Config) {
			if ns == "" {
				continue
			}
			b.namespaces[ns] = struct{}{}
		}
	}
	b.Config.Namespaces = make([]string, 0, internal.CapForLen(len(b.namespaces)))
	for ns := range b.namespaces {
		b.Config.Namespaces = append(b.Config.Namespaces, ns)
	}
	logger.Debug1("namespaces: %+v", b.Config.Namespaces)
	if org, ok := b.ModelContext.(NamespaceOrganizer); ok {
		b.Config.Namespaces = org.OrganizeNamespaces(b.Config.Namespaces)
	}
	return
}

type dbSchemaTableColumn struct {
	dbName  string
	dbCfg   config.Database
	db      *Database
	schName string
	schCfg  config.Schema
	schema  *Schema
	tblName string
	tblCfg  config.Table
	table   *Table
	colName string
	colCfg  config.Column
	column  *Column
}

func (b *configBuilder) iterDBSchemaTableColumn(c *config.Config, f func(dbSchemaTableColumn) error) error {
	for dbName, dbCfg := range c.Databases {
		db := b.Config.DatabasesByName[dbName]
		for schName, schCfg := range dbCfg.Schemas {
			schema := db.SchemasByName[schName]
			for tblName, tblCfg := range schCfg.Tables {
				table := schema.TablesByName[tblName]
				for colName, colCfg := range tblCfg.Columns {
					column := table.ColumnsByName[colName]
					if err := f(dbSchemaTableColumn{
						dbName, dbCfg, db,
						schName, schCfg, schema,
						tblName, tblCfg, table,
						colName, colCfg, column,
					}); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (b *configBuilder) getPathUp(path string, start *Table) (interface{}, error) {
	hops := strings.Count(path, ".")
	var root interface{}
	switch hops {
	case 0:
		root = start
	case 1:
		root = start.Schema
	case 2:
		root = start.Schema.Database
	case 3:
		root = start.Schema.Database.Config
	default:
		return nil, errors.Errorf("%q does not seem to be a path")
	}
	return b.getPathDown(path, root)
}

func (b *configBuilder) getPathDown(path string, start interface{}) (interface{}, error) {
	parts := strings.Split(path, ".")
	hop := start
	var ok bool
	for len(parts) > 0 {
		last := hop
		name := parts[0]
		parts = parts[1:]
		switch x := hop.(type) {
		case *Config:
			hop, ok = x.DatabasesByName[name]
		case *Database:
			hop, ok = x.SchemasByName[name]
		case *Schema:
			hop, ok = x.TablesByName[name]
		case *Table:
			hop, ok = x.ColumnsByName[name]
		default:
			return nil, errors.Errorf(
				"cannot get %[1]q from %[2]v "+
					"(type: %[2]T):  %[3]v "+
					"(type: %[3]T) has no %[4]q "+
					"member",
				path, last, hop, name)
		}
		if !ok {
			return nil, errors.Errorf(
				"failed to get %q from %v",
				name, last,
			)
		}
	}
	return hop, nil
}

func (b *configBuilder) newColumn(t *Table, name string, cfg *config.Column) (c *Column) {
	if len(b.caches.columns) == cap(b.caches.columns) {
		b.caches.columns = make([]Column, 1024)
	}
	c = &b.caches.columns[0]
	b.caches.columns = b.caches.columns[1:]
	c.Table = t
	c.Names.init(name, &t.Database.Namers.Column)
	return
}

func (b *configBuilder) newIDs(ids []*TableID) (keyIDs []*TableID) {
	const defaultKeyIDCap = 16
	if len(b.caches.keyIDs)+len(ids) > cap(b.caches.keyIDs) {
		if len(ids) > defaultKeyIDCap {
			keyIDs = make([]*TableID, len(ids))
			copy(keyIDs, ids)
			return
		}
		b.caches.keyIDs = make([]*TableID, defaultKeyIDCap)
	}
	keyIDs = b.caches.keyIDs[:len(ids):len(ids)]
	copy(keyIDs, ids)
	return
}

func (b *configBuilder) newID(c *Column, cfg *config.Column) (id *TableID) {
	if len(b.caches.ids) == cap(b.caches.ids) {
		b.caches.ids = make([]TableID, 128)
	}
	id = &b.caches.ids[0]
	b.caches.ids = b.caches.ids[1:]
	id.Column = c
	id.Names.init(c.RawName, &c.Table.Database.Namers.ID)
	return
}

func (b *configBuilder) newKey(t *Table, ids []*TableID) (key *TableKey) {
	if len(b.caches.keys) == cap(b.caches.keys) {
		b.caches.keys = make([]TableKey, 16)
	}
	key = &b.caches.keys[0]
	b.caches.keys = b.caches.keys[1:]
	key.Names.init(t.RawName+"Key", &t.Database.Namers.Key)
	key.IDs = b.newIDs(ids)
	return
}

func (b *configBuilder) newTable(s *Schema, name string, c *config.Table) (t *Table) {
	if len(b.caches.tables) == cap(b.caches.tables) {
		b.caches.tables = make([]Table, 128)
	}
	t = &b.caches.tables[0]
	b.caches.tables = b.caches.tables[1:]
	t.Schema = s
	t.Names.init(name, &s.Database.Namers.Table)
	t.Columns = make([]*Column, 0, len(c.Columns))
	t.ColumnsByName = make(map[string]*Column, len(c.Columns))
	return
}

func (b *configBuilder) newSchema(d *Database, name string, c *config.Schema) (s *Schema) {
	if len(b.caches.schemas) == cap(b.caches.schemas) {
		b.caches.schemas = make([]Schema, 8)
	}
	s = &b.caches.schemas[0]
	b.caches.schemas = b.caches.schemas[1:]
	s.Database = d
	s.Names.init(name, &d.Namers.Schema)
	s.Tables = make([]*Table, 0, len(c.Tables))
	s.TablesByName = make(map[string]*Table, len(c.Tables))
	return
}

func (b *configBuilder) newDatabase(name string, c *config.Database) (d *Database, err error) {
	if len(b.caches.databases) == cap(b.caches.databases) {
		b.caches.databases = make([]Database, 4)
	}
	d = &b.caches.databases[0]
	b.caches.databases = b.caches.databases[1:]
	d.Config = b.Config
	d.Names.init(name, &b.DatabaseNamers)
	d.Schemas = make([]*Schema, 0, len(c.Schemas))
	d.SchemasByName = make(map[string]*Schema, len(c.Schemas))
	initNamers := func(ofWhat string, nrs *Namers, c *config.Namers) (err error) {
		if err = nrs.init(c); err != nil {
			return errors.Errorf1From(
				err, "failed to initialize %s", ofWhat)
		}
		return
	}
	if err = initNamers("column", &d.Namers.Column, &c.Namers.Column); err != nil {
		return
	}
	if err = initNamers("id", &d.Namers.ID, &c.Namers.IDType); err != nil {
		return
	}
	if err = initNamers("key", &d.Namers.Key, &c.Namers.KeyType); err != nil {
		return
	}
	if err = initNamers("table", &d.Namers.Table, &c.Namers.Table); err != nil {
		return
	}
	if err = initNamers("schema", &d.Namers.Schema, &c.Namers.Schema); err != nil {
		return
	}
	return
}

type nopNamer struct{}

func (nopNamer) Apply(s string) string { return s }
func (nopNamer) Parse(s string) string { return s }

// RawPathToFK gets the dotted path from c to c's FK.
func RawPathToFK(c *Column) string {
	fkDB, fkSch, fkTbl, fkCol := elemPathToFK(c)
	var parts [4]string
	parts[3] = fkCol.RawName
	length := 1
	if fkTbl != nil {
		parts[2] = fkTbl.RawName
		length++
		if fkSch != nil {
			parts[1] = fkSch.RawName
			length++
			if fkDB != nil {
				parts[0] = fkDB.RawName
				length++
			}
		}
	}
	return strings.Join(parts[len(parts)-length:], ".")
}

func elemPathToFK(c *Column) (fkDB *Database, fkSchema *Schema, fkTabl *Table, fkColumn *Column) {
	fk := c.FK
	if fk == nil {
		return
	}
	fkColumn = fk.Col
	if fkColumn == nil || fkColumn == c {
		return
	}
	fkTable = fkColumn.Table
	cTable := c.Table
	if fkTable == nil || fkTable == cTable {
		return
	}
	fkSchema = fkTable.Schema
	cSchema := cTable.Schema
	if fkSchema == nil || fkSchema == cSchema {
		return
	}
	fkDB = fkSchema.Database
	return
}
