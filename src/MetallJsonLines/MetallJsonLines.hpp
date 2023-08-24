
#pragma once

#include <string_view>
#include <utility>

#include <metall/container/vector.hpp>
#include <metall/json/json.hpp>
#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>

#include <ygm/comm.hpp>
#include <ygm/io/csv_parser.hpp>

#include <experimental/cxx-compat.hpp>

#include "json_bento/box.hpp"

namespace msg {

struct process_data_mjl {
  // repeated from metall_json_lines class
  using lines_type =
      json_bento::box<metall::manager::allocator_type<std::byte> >;
  using accessor_type = lines_type::value_accessor;

  using projector_type =
      std::function<boost::json::value(const accessor_type&)>;

  lines_type*               vector;
  std::vector<std::string>* remoteRows;
  std::vector<std::size_t>* selectedRows;
  projector_type*           projector;
};

process_data_mjl mjlState;

//
// metall_json_lines::head messages

struct row_response {
  void operator()(std::vector<std::string> rows) {
    assert(mjlState.remoteRows != nullptr);

    std::move(rows.begin(), rows.end(),
              std::back_inserter(*mjlState.remoteRows));
  }
};

struct row_request {
  void operator()(ygm::comm* w, std::size_t numrows) const {
    assert(w != nullptr);
    assert(mjlState.vector != nullptr);
    assert(mjlState.selectedRows != nullptr);

    ygm::comm& world     = *w;
    const int  fromThis  = std::min(mjlState.selectedRows->size(), numrows);
    const int  fromOther = numrows - fromThis;

    if ((fromOther > 0) && (world.size() != (world.rank() + 1)))
      world.async(world.rank() + 1, row_request{}, fromOther);

    mjlState.selectedRows->resize(fromThis);

    std::vector<std::string> response;

    for (std::uint64_t i : *mjlState.selectedRows) {
      std::stringstream serial;

      serial << (*mjlState.projector)(mjlState.vector->at(i)) << std::flush;
      response.emplace_back(serial.str());
    }

    world.async(0, row_response{}, response);
  }
};

//
// metall_json_lines::info reduction operator

struct info_reduction {
  std::vector<int> operator()(const std::vector<int>& lhs,
                              const std::vector<int>& rhs) const {
    std::vector<int> res{lhs.begin(), lhs.end()};

    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
  }
};

}  // namespace msg

namespace {

template <class Fn, class Vector>
void _simple_for_all_selected(Fn fn, Vector& vector, std::size_t maxrows) {
  std::size_t const lim = std::min(vector.size(), maxrows);

  for (std::size_t i = 0; lim != i; ++i) fn(i, vector.at(i));
}

// can be invoked with const and non-const arguments
template <class Fn, class Vector, class FilterFns>
void _for_all_selected(
    Fn fn, Vector& vector, FilterFns& filterfn,
    std::size_t maxrows = std::numeric_limits<std::size_t>::max()) {
  if (filterfn.empty())
    return _simple_for_all_selected<Fn>(std::move(fn), vector, maxrows);

  std::size_t const lim = std::min(vector.size(), maxrows);
  std::size_t       i   = 0;

  while (maxrows && (lim != i)) {
    try {
      auto       filpos   = filterfn.begin();
      auto const fillim   = filterfn.end();
      auto       accElemI = vector.at(i);

      filpos = std::find_if_not(
          filpos, fillim,
          [i, &accElemI](typename FilterFns::value_type const& filter) -> bool {
            return filter(i, accElemI);
          });

      if (fillim == filpos) {
        fn(i, accElemI);
        --maxrows;
      }
    } catch (...) { /* \todo filter functions must not throw */
    }

    ++i;
  }
}

}  // namespace

namespace experimental {

template <class T>
T& checked_deref(T* ptr, const char* errmsg) {
  if (ptr) {
    CXX_LIKELY;
    return *ptr;
  }

  throw std::runtime_error("Unable to open metall_json_lines");
}

struct import_summary : std::tuple<std::size_t, std::size_t> {
  using base = std::tuple<std::size_t, std::size_t>;
  using base::base;

  std::size_t imported() const { return std::get<0>(*this); }
  std::size_t rejected() const { return std::get<1>(*this); }

  boost::json::object asJson() const {
    boost::json::object res;

    res["imported"] = imported();
    res["rejected"] = rejected();

    return res;
  }
};

struct metall_json_lines {
  using lines_type =
      json_bento::box<metall::manager::allocator_type<std::byte> >;
  using accessor_type = lines_type::value_accessor;

  using metall_allocator_type =
      decltype(std::declval<metall::utility::metall_mpi_adaptor>()
                   .get_local_manager()
                   .get_allocator());
  using filter_type  = std::function<bool(std::size_t, const accessor_type&)>;
  using updater_type = std::function<void(std::size_t, accessor_type)>;
  using visitor_type = std::function<void(std::size_t, const accessor_type&)>;
  using metall_projector_type =
      std::function<boost::json::value(const accessor_type&)>;
  using metall_manager_type = metall::utility::metall_mpi_adaptor;

  //
  // ctors

  metall_json_lines(metall_manager_type& mgr, ygm::comm& world)
      : ygmcomm(world),
        metallmgr(mgr),
        vector(checked_deref(metallmgr.get_local_manager()
                                 .find<lines_type>(metall::unique_instance)
                                 .first,
                             ERR_OPEN)) {}

  metall_json_lines(metall_manager_type& mgr, ygm::comm& world, const char* key)
      : ygmcomm(world),
        metallmgr(mgr),
        vector(checked_deref(
            metallmgr.get_local_manager().find<lines_type>(key).first,
            ERR_OPEN)) {}

  metall_json_lines(metall_manager_type& mgr, ygm::comm& world,
                    std::string_view key)
      : metall_json_lines(mgr, world, key.data()) {}

  //
  // accessors

  /// returns \ref numrows elements from the container
  boost::json::array head(std::size_t           numrows,
                          metall_projector_type projector) const {
    using ResultType = decltype(head(numrows, projector));

    ResultType               res;
    std::vector<std::string> remoteRows;
    std::vector<std::size_t> selectedRows;

    msg::mjlState =
        msg::process_data_mjl{&vector, &remoteRows, &selectedRows, &projector};

    // phase 1: make all local selections
    {
      for_all_selected(
          [&selectedRows](int rownum, const accessor_type&) -> void {
            selectedRows.emplace_back(rownum);
          },
          numrows);

      ygmcomm.barrier();
    }

    // phase 2: send data request to neighbor
    //          (cascades until numrows are available, or last rank)
    //          rank 0: start filling result vector
    {
      if (isMainRank() && (selectedRows.size() < numrows) && !isLastRank())
        ygmcomm.async(ygmcomm.rank() + 1, msg::row_request{},
                      (numrows - selectedRows.size()));

      for (std::uint64_t i : selectedRows)
        res.emplace_back(projector(vector.at(i)));

      ygmcomm.barrier();
    }

    // phase 3: append received data to res (only rank 0 receives data)
    for (const std::string& row : remoteRows)
      res.emplace_back(boost::json::parse(row));

    return res;
  }

  /// returns the number of elements in the local container
  std::size_t local_size() const { return vector.size(); }

  /// calls \ref accessor with each row, for up to \ref maxrows (per local
  /// container) times
  void for_all_selected(
      visitor_type accessor,
      std::size_t  maxrows = std::numeric_limits<std::size_t>::max()) const {
    _for_all_selected(std::move(accessor), vector, filterfn, maxrows);
  }

  /// returns the number of selected elements in the local container
  std::size_t count_selected() const {
    std::size_t selected = local_size();

    if (filterfn.size()) {
      selected = 0;
      for_all_selected([&selected](std::size_t, const accessor_type&) -> void {
        ++selected;
      });
    }

    return selected;
  }

  /// returns information about the data stored in each partition
  boost::json::array info() const {
    boost::json::array res;
    std::size_t        total = vector.size();

    // phase 1: count locally
    std::size_t selected = count_selected();

    // phase 2: reduce globally
    //          rank 0: produce result object
    {
      std::vector<int> inf = {int(ygmcomm.rank()), int(total), int(selected)};

      inf = ygmcomm.all_reduce(inf, msg::info_reduction{});

      if (isMainRank()) {
        for (std::size_t i = 0; i < inf.size(); i += 3) {
          boost::json::object obj;

          obj["rank"]     = inf.at(i);
          obj["elements"] = inf.at(i + 1);
          obj["selected"] = inf.at(i + 2);

          res.emplace_back(std::move(obj));
        }
      }
    }

    return res;
  }

  /// returns the total number of elements
  std::size_t count() const {
    // phase 1: count locally
    std::size_t selected = count_selected();

    // phase 2: reduce globally
    std::size_t totalSelected = ygmcomm.all_reduce_sum(selected);

    return totalSelected;
  }

  //
  // mutators

  /// clears the local container
  void clear() { vector.clear(); }

  /// calls updater(row) for each selected row
  /// \param  updater a function that may modify an JSON line
  /// \return the number of updated lines
  /// \pre An updater function \ref updater must not throw
  std::size_t set(updater_type updater) {
    std::size_t updcount = 0;

    // phase 1: update records locally
    {
      _for_all_selected(
          [&updcount, fn = std::move(updater)](int           rownum,
                                               accessor_type obj) -> void {
            ++updcount;
            fn(rownum, obj);
          },
          vector, filterfn);
    }

    // phase 2: compute total update count
    std::size_t res = ygmcomm.all_reduce_sum(updcount);

    return res;
  }

  /// imports json files and returns the number of imported rows
  /// \param  files       a list of JSON data files that will be imported
  /// \param  filter      a function that accepts or rejects a JSON line
  /// \param  transformer a function that transforms a JSON entry before it is
  /// stored \return a summary of how many lines were imported and rejected.
  import_summary read_json_files(
      const std::vector<std::string>&                       files,
      std::function<bool(const boost::json::value&)>        filter = accept_all,
      std::function<boost::json::value(boost::json::value)> transformer =
          identity_transformer) {
    // namespace mtljsn = metall::json::json;

    // phase 1: distributed import of data in files
    ygm::io::line_parser  lineParser{ygmcomm, files};
    std::size_t           imported    = 0;
    std::size_t           rejected    = 0;
    std::size_t const     initialSize = vector.size();
    lines_type*           vec         = &vector;
    metall_allocator_type alloc       = get_allocator();

    lineParser.for_all(
        [&imported, &rejected, vec, alloc, filterFn = std::move(filter),
         transFn = std::move(transformer)](const std::string& line) -> void {
          boost::json::value jsonLine = boost::json::parse(line);

          if (filterFn(jsonLine)) {
            vec->push_back(transFn(std::move(jsonLine)));
            ++imported;
          } else {
            ++rejected;
          }
        });

    assert(vec->size() == initialSize + imported);

    // phase 2: compute total number of imported rows
    int totalImported = ygmcomm.all_reduce_sum(imported);
    int totalRejected = ygmcomm.all_reduce_sum(rejected);

    return {totalImported, totalRejected};
  }

  /// imports a json files and returns the number of imported rows
  import_summary read_json_file(std::string file) {
    std::vector<std::string> files;

    files.emplace_back(std::move(file));
    return read_json_files(files);
  }

  //
  // filter setters

  /// appends filters and returns *this
  /// \pre A filter \ref fn must not throw
  /// \note *this is returned to allow operation chaining on the container
  ///       e.g., mjl.filter(...).count();
  /// \{
  metall_json_lines& filter(filter_type fn) {
    filterfn.emplace_back(std::move(fn));
    return *this;
  }

  metall_json_lines& filter(std::vector<filter_type> fns) {
    std::move(fns.begin(), fns.end(), std::back_inserter(filterfn));
    return *this;
  }
  /// \}

  /// resets the filter
  void clearFilter() { filterfn.clear(); }

  //
  // local access/mutator functions

  /// returns the local element at index \ref idx
  /// \{
  accessor_type       at(std::size_t idx) { return vector.at(idx); }
  accessor_type const at(std::size_t idx) const { return vector.at(idx); }
  /// \}

  /// appends a single element to the local container
  accessor_type append_local(boost::json::value val) {
    vector.push_back(std::move(val));
    return vector.back();
  }
  accessor_type append_local() { return append_local(boost::json::value{}); }

  //
  // others

  /// returns the metall allocator
  metall_allocator_type get_allocator() {
    return metallmgr.get_local_manager().get_allocator();
  }

  /// returns the communicator
  ygm::comm& comm() const { return ygmcomm; }

  //
  // static creators

  static void create_new(metall_manager_type& manager, ygm::comm&) {
    auto&       mgr = manager.get_local_manager();
    const auto* vec =
        mgr.construct<lines_type>(metall::unique_instance)(mgr.get_allocator());

    checked_deref(vec, ERR_CONSTRUCT);
  }

  static void create_new(metall_manager_type&          manager, ygm::comm&,
                         std::vector<std::string_view> metallkeys) {
    auto& mgr = manager.get_local_manager();

    for (std::string_view key : metallkeys) {
      const auto* vec =
          mgr.construct<lines_type>(key.data())(mgr.get_allocator());

      checked_deref(vec, ERR_CONSTRUCT);
    }
  }

  static void create_new(metall_manager_type& manager, ygm::comm& comm,
                         std::string_view metallkey) {
    create_new(manager, comm, {metallkey});
  }

  static void check_state(metall_manager_type& manager, ygm::comm&) {
    auto&       mgr = manager.get_local_manager();
    const auto* vec = mgr.find<lines_type>(metall::unique_instance).first;

    checked_deref(vec, ERR_OPEN);
  }

  static void check_state(metall_manager_type&          manager, ygm::comm&,
                          std::vector<std::string_view> keys) {
    auto& mgr = manager.get_local_manager();

    for (std::string_view key : keys) {
      const auto* vec = mgr.find<lines_type>(key.data()).first;

      checked_deref(vec, ERR_OPEN);
    }
  }

  static void check_state(metall_manager_type& manager, ygm::comm& comm,
                          std::string_view key) {
    check_state(manager, comm, {key});
  }

  static bool accept_all(const boost::json::value&) { return true; }

  static boost::json::value identity_transformer(boost::json::value val) {
    return val;
  }

 private:
  ygm::comm&                           ygmcomm;
  metall::utility::metall_mpi_adaptor& metallmgr;
  lines_type&                          vector;
  std::vector<filter_type>             filterfn = {};

  bool isMainRank() const { return 0 == ygmcomm.rank(); }
  bool isLastRank() const { return 1 == ygmcomm.size() - ygmcomm.rank(); }

  static constexpr char const* ERR_OPEN =
      "unable to open metall_json_lines object";
  static constexpr char const* ERR_CONSTRUCT =
      "unable to construct metall_json_lines object";

  metall_json_lines()                                    = delete;
  metall_json_lines(metall_json_lines&&)                 = delete;
  metall_json_lines(const metall_json_lines&)            = delete;
  metall_json_lines& operator=(metall_json_lines&&)      = delete;
  metall_json_lines& operator=(const metall_json_lines&) = delete;
};

}  // namespace experimental
