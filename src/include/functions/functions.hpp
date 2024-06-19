//===----------------------------------------------------------------------===//
//                         Scrooge
//
// functions/functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace teehr {

struct FirstScrooge {
  static void RegisterFunction(duckdb::Connection &conn,
                               duckdb::Catalog &catalog);
};

struct NashSutcliffe {
  static void RegisterFunction(duckdb::Connection &conn,
                               duckdb::Catalog &catalog);
};

// struct Aliases {
//   static void Register(
//     duckdb::Connection &conn,
//     duckdb::Catalog &catalog);
// };

} // namespace hydro_duck