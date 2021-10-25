#include <sqlite3.h>
#include "Svar.h"

inline int SQLite3CallHelper(const int result_code, const std::string& filename,
                             const int line_number) {
  switch (result_code) {
    case SQLITE_OK:
    case SQLITE_ROW:
    case SQLITE_DONE:
      return result_code;
    default:
      fprintf(stderr, "SQLite error [%s, line %i]: %s\n", filename.c_str(),
              line_number, sqlite3_errstr(result_code));
      throw sv::SvarExeption("SQLite error");
  }
}

#define SQLITE3_CALL(func) SQLite3CallHelper(func, __FILE__, __LINE__)

#define SQLITE3_EXEC(database, sql, callback)                                 \
  {                                                                           \
    char* err_msg = nullptr;                                                  \
    const int result_code =                                                   \
        sqlite3_exec(database, sql, callback, nullptr, &err_msg);             \
    if (result_code != SQLITE_OK) {                                           \
      fprintf(stderr, "SQLite error [%s, line %i]: %s\n", __FILE__, __LINE__, \
              err_msg);                                                       \
      sqlite3_free(err_msg);                                                  \
    }                                                                         \
  }

class DatabaseCurser{
public:
    DatabaseCurser(sqlite3_stmt* sql_stmt)
        : sql_stmt(sql_stmt), state(SQLITE_DONE){}

    ~DatabaseCurser(){
        SQLITE3_CALL(sqlite3_finalize(sql_stmt));
    }

    sv::Svar value2svar(sqlite3_value* value){
        switch(sqlite3_value_type(value))
        {
        case SQLITE_INTEGER:
            return sqlite3_value_int(value);
        case SQLITE_FLOAT:
            return sqlite3_value_double(value);
        case SQLITE_TEXT:
            return reinterpret_cast<const char*>(sqlite3_value_text(value));
        case SQLITE_BLOB:
            return sv::SvarBuffer(sqlite3_value_blob(value),sqlite3_value_bytes(value));
        case SQLITE_NULL:
            return sv::Svar::Null();
        default:
            return sv::Svar();
        }
    }

    sv::Svar fetchone(){
        if(state != SQLITE_ROW) return sv::Svar();
        std::vector<sv::Svar> col;
        for(int i=0,iend=sqlite3_column_count(sql_stmt);i<iend;++i){
            auto value = sqlite3_column_value(sql_stmt, i);
            col.push_back(value2svar(value));
        }
        state=SQLITE3_CALL(sqlite3_step(sql_stmt));
        return col;
    }

    sv::Svar fetchall(){
        if(state != SQLITE_ROW) return sv::Svar();
        std::vector<sv::Svar> rows;
        while(true){
            std::vector<sv::Svar> col;
            for(int i=0,iend=sqlite3_column_count(sql_stmt);i<iend;++i){
                auto value = sqlite3_column_value(sql_stmt, i);
                col.push_back(value2svar(value));
            }
            rows.push_back(col);
            state=SQLITE3_CALL(sqlite3_step(sql_stmt));
            if(state != SQLITE_ROW)
                break;
        }
        return rows;
    }

    DatabaseCurser* execute(sv::Svar values){
        sqlite3_reset(sql_stmt);

        for(int i=0;i<values.size();i++){
            sv::Svar v = values[i];
            switch (v.jsontype()) {
            case sv::integer_t:
                sqlite3_bind_int(sql_stmt, i+1, v.unsafe_as<int>());
                break;
            case sv::float_t:
                sqlite3_bind_double(sql_stmt, i+1, v.unsafe_as<double>());
                break;
            case sv::string_t:{
                std::string& str = v.unsafe_as<std::string>();
                sqlite3_bind_text(sql_stmt, i+1, str.c_str(), str.size(), SQLITE_STATIC);
                break;
            }
            case sv::buffer_t:{
                sv::SvarBuffer buf= v.as<sv::SvarBuffer>();
                sqlite3_bind_blob(sql_stmt, i+1, buf.ptr(), buf.size(), SQLITE_STATIC);
                break;
            }
            default:
                sqlite3_bind_null(sql_stmt, i+1);
                break;
            }
        }

        state = SQLITE3_CALL(sqlite3_step(sql_stmt));

        return this;
    }

private:
    DatabaseCurser(const DatabaseCurser& r){}

    sqlite3_stmt* sql_stmt;
    int           state;
};

class Database{
  public:
    Database(std::string path){
        // SQLITE_OPEN_NOMUTEX specifies that the connection should not have a
        // mutex (so that we don't serialize the connection's operations).
        // Modifications to the database will still be serialized, but multiple
        // connections can read concurrently.
        SQLITE3_CALL(sqlite3_open_v2(
                         path.c_str(), &database_,
                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
                         nullptr));

        // Don't wait for the operating system to write the changes to disk
        SQLITE3_EXEC(database_, "PRAGMA synchronous=OFF", nullptr);

        // Use faster journaling mode
        SQLITE3_EXEC(database_, "PRAGMA journal_mode=WAL", nullptr);

        // Store temporary tables and indices in memory
        SQLITE3_EXEC(database_, "PRAGMA temp_store=MEMORY", nullptr);

        // Disabled by default
        SQLITE3_EXEC(database_, "PRAGMA foreign_keys=ON", nullptr);
    }

    sv::Svar execute(std::string sql,sv::Svar values){
        sqlite3_stmt* stmt=nullptr;

        SQLITE3_CALL(sqlite3_prepare_v2(database_, sql.c_str(), -1,&stmt, 0));
        auto curser=std::make_shared<DatabaseCurser>(stmt);
        curser->execute(values);
        return curser;
    }

    sqlite3* database_ = nullptr;
};

REGISTER_SVAR_MODULE(sqlite3){
    using namespace sv;
    sv::Class<DatabaseCurser>("DatabaseCurser")
            .def("fetchone",&DatabaseCurser::fetchone)
            .def("fetchall",&DatabaseCurser::fetchall)
            .def("execute",&DatabaseCurser::execute);

    sv::Class<Database>("Database")
            .construct<std::string>()
            .def("execute",&Database::execute,"sql"_a,"values"_a=sv::Svar::array());
}

EXPORT_SVAR_INSTANCE
