

#include <iostream>
#include <fstream>
#include <numeric>
#include <limits>

#include <boost/json.hpp>
#include <metall/container/experimental/json/parse.hpp>

#include "clippy/clippy.hpp"
#include "clippy/clippy-eval.hpp"
#include "mf-common.hpp"


namespace bjsn    = boost::json;
namespace mtlutil = metall::utility;
namespace mtljsn  = metall::container::experimental::json;
namespace jl      = json_logic;

static const std::string methodName     = "set";
static const std::string ARG_COLUMN     = "column";
static const std::string ARG_EXPRESSION = "expression";

/*
template <typename allocator_type>
bjsn::object
to_boost_object(const mj::object<allocator_type> &input) {
  bj::object object;
  for (const auto &elem : input) {
    object[elem.key().data()] = ;
  }
  return bj::serialize(object);
}
*/

template <class Alloc>
void
updateColumn(vector_json_type& dataset, const std::vector<int>& rows, const std::string& columnName, bjsn::object& jexp, Alloc alloc)
{
  auto [ast, vars, hasComputedVarNames] = json_logic::translateNode(jexp["rule"]);

  if (hasComputedVarNames) throw std::runtime_error("unable to work with computed variable names");

  for (std::int64_t rownum : rows)
  {
    auto&     rowobj = dataset.at(rownum).as_object();
    const int selLen = (SELECTOR.size() + 1);
    auto      varLookup = [&rowobj,selLen,rownum](const bjsn::string& colname, int) -> jl::ValueExpr
                          {
                            // \todo match selector instead of skipping it
                            std::string_view col{colname.begin() + selLen, colname.size() - selLen};
                            auto             pos = rowobj.find(col);

                            if (pos == rowobj.end())
                            {
                              CXX_UNLIKELY;
                              return (col == "rowid") ? jl::toValueExpr(rownum)
                                                      : jl::toValueExpr(nullptr);
                            }

                            return toValueExpr(pos->value());
                          };

    jl::ValueExpr exp = jl::calculate(ast, varLookup);

    std::stringstream jstr;

    jstr << exp;

    // return metall::container::experimental::json::value_from(std::move(bj_value), allocator);
    rowobj[columnName] = mtljsn::parse(jstr.str(), alloc);
    //~ std::cerr << "G" << rownum << std::endl;
    //~ auto vvv = mtljsn::parse(jstr.str(), alloc);
    //~ std::cerr << "H" << rownum << " " << vvv << std::endl;
  }
}


int ygm_main(ygm::comm& world, int argc, char** argv)
{
  int            error_code = 0;
  clippy::clippy clip{methodName, "For all selected rows, set a field to a (computed) value."};

  clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  clip.add_required<std::string>(ARG_COLUMN, "output column");
  clip.add_required<bjsn::object>(ARG_EXPRESSION, "output value expression");

  clip.add_required_state<std::string>(ST_METALL_LOCATION, "Metall storage location");

  if (clip.parse(argc, argv)) { return 0; }

  try
  {
    std::string                 dataLocation = clip.get_state<std::string>(ST_METALL_LOCATION);
    std::string                 columnName = clip.get<std::string>(ARG_COLUMN);
    bjsn::object                columnExpr = clip.get<bjsn::object>(ARG_EXPRESSION);
    mtlutil::metall_mpi_adaptor manager(metall::open_only, dataLocation.c_str(), MPI_COMM_WORLD);
    vector_json_type&           vec = jsonVector(manager);
    const std::vector<int>      selectedRows = getSelectedRows(clip, vec);
    auto&                       mgr = manager.get_local_manager();

    updateColumn(vec, selectedRows, columnName, columnExpr, mgr.get_allocator());
    world.barrier(); // necessary?

    const int                   totalUpdated = world.all_reduce_sum(selectedRows.size());

    if (world.rank() == 0)
    {
      std::stringstream msg;

      msg << "updated column " << columnName << " in " << totalUpdated << " entries"
          << std::endl;
      clip.to_return(msg.str());
    }
  }
  catch (const std::exception& err)
  {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}


