
#pragma once

#include <string_view>
#include <utility>

#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>
#include <metall/container/vector.hpp>
#include <metall/container/experimental/json/json.hpp>

#include <ygm/comm.hpp>
#include <ygm/io/csv_parser.hpp>

#include <experimental/cxx-compat.hpp>

namespace msg
{

struct ProcessData
{
  using value_type       = metall::container::experimental::json::value<metall::manager::allocator_type<std::byte>>;
  using lines_type       = metall::container::vector<value_type, metall::manager::scoped_allocator_type<value_type>>;
  using projector_type   = std::function<boost::json::value(const value_type&)>;

  lines_type*               vector;
  std::vector<std::string>* remoteRows;
  std::vector<std::size_t>* selectedRows;
  projector_type*           projector;
};

ProcessData state;


//
// MetallJsonLines::head messages

struct RowResponse
{
  void operator()(std::vector<std::string> rows)
  {
    assert(state.remoteRows != nullptr);

    std::move(rows.begin(), rows.end(), std::back_inserter(*state.remoteRows));
  }
};

struct RowRequest
{
  void operator()(ygm::comm* w, std::size_t numrows) const
  {
    assert(w != nullptr);
    assert(state.vector != nullptr);
    assert(state.selectedRows != nullptr);

    ygm::comm& world     = *w;
    const int  fromThis  = std::min(state.selectedRows->size(), numrows);
    const int  fromOther = numrows - fromThis;

    if ((fromOther > 0) && (world.size() != (world.rank()+1)))
      world.async( world.rank()+1, RowRequest{}, fromOther );

    state.selectedRows->resize(fromThis);

    std::vector<std::string> response;

    for (std::uint64_t i : *state.selectedRows)
    {
      std::stringstream serial;

      serial << (*state.projector)(state.vector->at(i)) << std::flush;
      response.emplace_back(serial.str());
    }

    world.async(0, RowResponse{}, response);
  }
};

//
// MetallJsonLines::info reduction operator

struct InfoReduction
{
  std::vector<int> operator()(const std::vector<int>& lhs, const std::vector<int>& rhs) const
  {
    std::vector<int> res{lhs.begin(), lhs.end()};

    res.insert(res.end(), rhs.begin(), rhs.end());
    return res;
  }
};


}

namespace
{

template <class Fn, class Vector>
void _simpleForAllSelected(Fn fn, Vector& vector, std::size_t maxrows)
{
  auto        pos = vector.begin();
  auto const  lim = pos+std::min(vector.size(), maxrows);
  std::size_t i   = 0;

  for (; pos != lim; ++pos, ++i) fn(i, *pos);
}

// can be invoked with const and non-const arguments
template < class Fn
         , class Vector
         , class FilterFns
         >
void _forAllSelected( Fn fn,
                      Vector& vector,
                      FilterFns& filterfn,
                      std::size_t maxrows = std::numeric_limits<std::size_t>::max()
                    )
{
  if (filterfn.empty())
    return _simpleForAllSelected<Fn>(std::move(fn), vector, maxrows);

  auto        pos = vector.begin();
  auto const  lim = vector.end();
  std::size_t i = 0;

  while (maxrows && (lim != pos))
  {
    auto       filpos = filterfn.begin();
    auto const fillim = filterfn.end();

    filpos = std::find_if_not( filpos, fillim,
                               [i, pos](typename FilterFns::value_type const& filter) -> bool
                               {
                                 return filter(i, *pos);
                               }
                             );

    if (fillim == filpos)
    {
      fn(i, *pos);
      --maxrows;
    }

    ++pos; ++i;
  }
}

}

namespace experimental
{

template <class T>
T& checked_deref(T* ptr, const char* errmsg)
{
  if (ptr)
  {
    CXX_LIKELY;
    return *ptr;
  }

  throw std::runtime_error("Unable to open MetallJsonLines");
}

struct MetallJsonLines
{
    using value_type            = metall::container::experimental::json::value<metall::manager::allocator_type<std::byte>>;
    using lines_type            = metall::container::vector<value_type, metall::manager::scoped_allocator_type<value_type>>;
    using metall_allocator_type = decltype(std::declval<metall::utility::metall_mpi_adaptor>().get_local_manager().get_allocator());
    using filter_type           = std::function<bool(std::size_t, const value_type&)>;
    using updater_type          = std::function<void(std::size_t, value_type&)>;
    using accessor_type         = std::function<void(std::size_t, const value_type&)>;
    using metall_projector_type = std::function<boost::json::value(const value_type&)>;
    //~ using functor_type          = std::function<void(std::size_t, value_type&)>;
    //~ using const_functor_type    = std::function<void(std::size_t, const value_type&)>;

    //
    // ctors

    template <class OpenTag = metall::open_read_only_t>
    MetallJsonLines(ygm::comm& world, OpenTag tag, const char* loc, const MPI_Comm& comm)
    : ygmcomm(world),
      metallmgr{tag, loc, comm},
      vector(checked_deref(metallmgr.get_local_manager().find<lines_type>(metall::unique_instance).first, ERR_OPEN))
    {}

    template <class OpenTag = metall::open_read_only_t>
    MetallJsonLines(ygm::comm& world, OpenTag tag, std::string_view loc, const MPI_Comm& comm)
    : MetallJsonLines(world, tag, loc.data(), comm)
    {}

    //
    // accessors

    /// returns \ref numrows elements from the container
    boost::json::array
    head(std::size_t numrows, metall_projector_type projector) const
    {
      using ResultType = decltype(head(numrows, projector));

      ResultType               res;
      std::vector<std::string> remoteRows;
      std::vector<std::size_t> selectedRows;

      msg::state = msg::ProcessData{&vector, &remoteRows, &selectedRows, &projector};

      // phase 1: make all local selections
      {
        forAllSelected( [&selectedRows](int rownum, const value_type&) -> void
                        {
                          selectedRows.emplace_back(rownum);
                        },
                        numrows
                      );

        ygmcomm.barrier();
      }

      // phase 2: send data request to neighbor
      //          (cascades until numrows are available, or last rank)
      //          rank 0: start filling result vector
      {
        if (isMainRank() && (selectedRows.size() < numrows) && !isLastRank())
          ygmcomm.async( ygmcomm.rank()+1, msg::RowRequest{}, (numrows-selectedRows.size()) );

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
    std::size_t countAllLocal() const
    {
      return vector.size();
    }

    /// calls \ref accessor with each row, for up to \ref maxrows (per local container) times
    void forAllSelected( accessor_type accessor,
                         std::size_t maxrows = std::numeric_limits<std::size_t>::max()
                       ) const
    {
      _forAllSelected( std::move(accessor), vector, filterfn, maxrows );
    }

    /// returns the number of selected elements in the local container
    std::size_t countSelected() const
    {
      std::size_t selected = countAllLocal();

      if (filterfn.size())
      {
        selected = 0;
        forAllSelected([&selected](std::size_t, const value_type&) -> void { ++selected; });
      }

      return selected;
    }

    /// returns information about the data stored in each partition
    boost::json::array
    info() const
    {
      boost::json::array res;
      std::size_t        total = vector.size();

      // phase 1: count locally
      std::size_t        selected = countSelected();

      // phase 2: reduce globally
      //          rank 0: produce result object
      {
        std::vector<int> inf = { int(ygmcomm.rank()), int(total), int(selected) };

        inf = ygmcomm.all_reduce(inf, msg::InfoReduction{});

        if (isMainRank())
        {
          for (std::size_t i = 0; i < inf.size(); i+=3)
          {
            boost::json::object obj;

            obj["rank"]     = inf.at(i);
            obj["elements"] = inf.at(i+1);
            obj["selected"] = inf.at(i+2);

            res.emplace_back(std::move(obj));
          }
        }
      }

      return res;
    }

    /// returns the total number of elements
    std::size_t count() const
    {
      // phase 1: count locally
      std::size_t selected = countSelected();

      // phase 2: reduce globally
      std::size_t totalSelected = ygmcomm.all_reduce_sum(selected);

      return totalSelected;
    }

    //
    // mutators

    /// clears the local container
    void clear() { vector.clear(); }

    /// calls updater(row) for each selected row
    std::size_t set(updater_type updater)
    {
      std::size_t updcount = 0;

      // phase 1: update records locally
      {
        _forAllSelected( [&updcount, fn = std::move(updater)](int rownum, value_type& obj) -> void
                         {
                           ++updcount;
                           fn(rownum, obj);
                         },
                         vector,
                         filterfn
                       );
      }

      // phase 2: compute total update count
      std::size_t res = ygmcomm.all_reduce_sum(updcount);

      return res;
    }

    /// imports json files and returns the number of imported rows
    std::size_t
    readJsonFiles(const std::vector<std::string>& files)
    {
      namespace mtljsn = metall::container::experimental::json;

      // phase 1: distributed import of data in files
      ygm::io::line_parser        lineParser{ ygmcomm, files };
      std::size_t                 imported    = 0;
      std::size_t const           initialSize = vector.size();
      lines_type*                 vec = &vector;
      metall_allocator_type       alloc = get_allocator();

      lineParser.for_all( [&imported, vec, alloc]
                          (const std::string& line) -> void
                          {
                            vec->emplace_back(mtljsn::parse(line, alloc));
                            ++imported;
                          }
                        );

      assert(vec->size() == initialSize + imported);

      // phase 2: compute total number of imported rows
      int totalImported = ygmcomm.all_reduce_sum(imported);

      return totalImported;
    }

    /// imports a json files and returns the number of imported rows
    std::size_t
    readJsonFile(std::string file)
    {
      std::vector<std::string> files;

      files.emplace_back(std::move(file));
      return readJsonFiles(files);
    }



    //
    // filter setters

    /// appends filters and returns *this
    /// \note *this is returned to allow operation chaining on the container
    ///       e.g., mjl.filter(...).count();
    /// \{
    MetallJsonLines& filter(filter_type fn)
    {
      filterfn.emplace_back(std::move(fn));
      return *this;
    }

    MetallJsonLines& filter(std::vector<filter_type> fns)
    {
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
    value_type&       at(std::size_t idx)       { return vector.at(idx); }
    value_type const& at(std::size_t idx) const { return vector.at(idx); }
    /// \}

    /// appends a single element to the local container
    value_type& append_local()                  { vector.emplace_back(); return vector.back(); }
    value_type& append_local(value_type&& val)  { vector.emplace_back(std::move(val)); return vector.back(); }

    //
    // others

    /// returns the metall allocator
    metall_allocator_type get_allocator()
    {
      return metallmgr.get_local_manager().get_allocator();
    }

    /// returns the communicator
    ygm::comm& comm() const { return ygmcomm; }


    //
    // static creators

    static
    void createOverwrite(ygm::comm& world, std::string_view loc, const MPI_Comm& comm)
    {
      if (std::filesystem::is_directory(loc.data()))
        std::filesystem::remove_all(loc.data());

      createInternal(world, loc, comm);
    }

    static
    void createNewOnly(ygm::comm& world, std::string_view loc, const MPI_Comm& comm)
    {
      if (!metall::utility::metall_mpi_adaptor::consistent(loc.data(), MPI_COMM_WORLD))
      {
        // \todo call createOverwrite instead?
        createInternal(world, loc, comm);
      }
    }

  private:
    ygm::comm&                          ygmcomm;
    metall::utility::metall_mpi_adaptor metallmgr;
    lines_type&                         vector;
    std::vector<filter_type>            filterfn = {};

    bool isMainRank() const { return 0 == ygmcomm.rank(); }
    bool isLastRank() const { return 1 == ygmcomm.size() - ygmcomm.rank(); }

    static constexpr char const* ERR_OPEN = "unable to open MetallJsonLines object";

    static
    void createInternal(ygm::comm& world, std::string_view loc, const MPI_Comm& comm)
    {
      {
        metall::utility::metall_mpi_adaptor manager(metall::create_only, loc.data(), MPI_COMM_WORLD);
        auto&                               mgr = manager.get_local_manager();
      /*const auto*                         vec = */ mgr.construct<lines_type>(metall::unique_instance)(mgr.get_allocator());
      }
    }

    MetallJsonLines()                                  = delete;
    MetallJsonLines(MetallJsonLines&&)                 = delete;
    MetallJsonLines(const MetallJsonLines&)            = delete;
    MetallJsonLines& operator=(MetallJsonLines&&)      = delete;
    MetallJsonLines& operator=(const MetallJsonLines&) = delete;
};

}
