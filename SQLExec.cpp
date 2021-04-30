/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    // FIXME
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

// create table
// .py line 100
QueryResult *SQLExec::create(const CreateStatement *statement) {

    string table_name = statement->; //not sure what statement can point to
    string columns = statement->;

    // look at HeapTable.cpp 
    // in storage_engine.h
    // update _tables schema
    ValueDict* t = new ValueDict();
    Value v_tableName = Value(table_name);
    (*t)[table_name] = v_tableName;
    tables->insert(t);

    try {
        // update _columns schema
        for(c: )
        {

        }
        try {
            for(column_name: Columns)

        } catch() {

        }


    } catch(DbRelationError) {

    }

    return new QueryResult("not implemented"); // FIXME
}

// drop table
// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    if(statement->type != hsql::DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");
    
    string table_name = statement->; 
    if (table_name. TABLE_NAME)//in schema tables.h
    {
        throw SQLExecError("Cannot drop a schema table!");
    }


    delete handles;

    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult("not implemented"); // FIXME
}


QueryResult *SQLExec::show(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

// show tables
QueryResult *SQLExec::show_tables() {
    return new QueryResult("not implemented"); // FIXME
}

// show columns
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // .py line 58
    string table_name = statement->;
    


    return new QueryResult("not implemented"); // FIXME
}

