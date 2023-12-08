// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief This file contains common code for the MetallFrame implementation

#pragma once


#include <metall/metall.hpp>
#include <metall/utility/metall_mpi_adaptor.hpp>

#include <ygm/comm.hpp>
#include <ygm/io/csv_parser.hpp>

#include <experimental/dataframe.hpp>
#include "csv-line-io.hpp"

namespace xpr = ::experimental;

namespace msg
{
struct process_data_mf {
  // repeated from metall_json_lines class
  using lines_type     = experimental::DataFrame;
  using row_type       = std::vector<experimental::dataframe_variant_t>;
  using projector_type = std::function<row_type(const row_type&)>;

  lines_type*               vector;
  std::vector<row_type>*    remoteRows;
  std::vector<std::size_t>* selectedRows;
  projector_type*           projector;
};

process_data_mf mfState;

struct row_response {
  using row_type = process_data_mf::row_type;

  void operator()(std::vector<row_type> rows) {
    assert(mfState.remoteRows != nullptr);

    std::move(rows.begin(), rows.end(),
              std::back_inserter(*mfState.remoteRows));
  }
};

struct row_request {
  void operator()(ygm::comm* w, std::size_t numrows) const {
    assert(w != nullptr);
    assert(mfState.vector != nullptr);
    assert(mfState.selectedRows != nullptr);

    ygm::comm& world     = *w;
    const int  fromThis  = std::min(mfState.selectedRows->size(), numrows);
    const int  fromOther = numrows - fromThis;

    if ((fromOther > 0) && (world.size() != (world.rank() + 1)))
    {
      world.async(world.rank() + 1, row_request{}, fromOther);
    }

    mfState.selectedRows->resize(fromThis);

    std::vector<std::vector<experimental::dataframe_variant_t> > response;

    for (std::uint64_t i : *mfState.selectedRows) {
      // response.emplace_back((*mfState.projector)(mfState.frame->at(i)));
    }

    world.async(0, row_response{}, response);
  }
};

namespace
{


bool check_condition(bool v, const char* errmsg = "Failed check") {
  if (!v) {
    CXX_UNLIKELY;
    throw std::runtime_error(errmsg);
  }

  return v;
}


std::vector<int>
all_column_indices(const experimental::DataFrame& df)
{
  const int        numcol = df.columns();
  std::vector<int> res(numcol);

  std::iota(res.begin(), res.end(), 0);
  return res;
}


using DataConverter = std::function<xpr::dataframe_variant_t(const std::string& s)>;

template <class DfCellType>
struct ConverterFn
{
  ConverterFn(xpr::DataFrame* /* not needed */)
  {}

  xpr::dataframe_variant_t operator()(const std::string& s)
  {
    // return boost::lexical_cast<DfCellType>(s);
    std::stringstream inp(s);
    DfCellType        val;

    inp >> val;
    return val;
  }
};

template <>
struct ConverterFn<xpr::string_t>
{
  xpr::dataframe_variant_t operator()(const std::string& s)
  {
    return df->persistent_string(s);
  }

  xpr::DataFrame* df;
};


template <class ColType>
bool push_conv_if(std::vector<DataConverter>& res, xpr::dataframe& df, const xpr::column_desc& coldesc)
{
  if (!coldesc.is<ColType>()) return false;

  res.push_back(ConverterFn<ColType>{&df});
  return true;
}

std::vector<DataConverter>
mk_data_converter(xpr::dataframe& df)
{
  const std::vector<int>              colIndcs = all_column_indices(df);
  const std::vector<xpr::column_desc> colDescs = df.get_column_descriptors(colIndcs);
  std::vector<DataConverter>          res;

  for (const xpr::column_desc& col : colDescs)
  {
       push_conv_if<xpr::int_t>   (res, df, col)
    || push_conv_if<xpr::uint_t>  (res, df, col)
    || push_conv_if<xpr::real_t>  (res, df, col)
    || push_conv_if<xpr::string_t>(res, df, col)
    || check_condition(false, "invalid column type");
  }

  return res;
}

template <class Fn, class DataFrame>
void _simple_for_all_selected(Fn fn, DataFrame& df, std::size_t maxrows) {
  std::size_t const lim = std::min(df.size(), maxrows);

  for (std::size_t i = 0; lim != i; ++i) fn(i, df.at(i));
}

// can be invoked with const and non-const arguments
template <class Fn, class DataFrame, class FilterFns>
void _for_all_selected(
    Fn fn, DataFrame& df, FilterFns& filterfn,
    std::size_t maxrows = std::numeric_limits<std::size_t>::max()) {
#if 0
    if (filterfn.empty())
      return _simple_for_all_selected<Fn>(std::move(fn), df, maxrows);

    std::size_t const lim = std::min(df.rows(), maxrows);
    std::size_t       i   = 0;

    while (maxrows && (lim != i)) {
      try {
        auto       filpos   = filterfn.begin();
        auto const fillim   = filterfn.end();
        auto       accElemI = df.at(i);

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
#endif
}


}

namespace experimental {

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

  struct metall_frame {
      using metall_manager_type = metall::utility::metall_mpi_adaptor;
      using lines_type          = xpr::dataframe;

      // supported column types
      using string_t       = xpr::string_t;
      using int_t          = xpr::int_t;
      using uint_t         = xpr::uint_t;
      using real_t         = xpr::real_t;
      using data_variant_t = xpr::dataframe_variant_t;
      using row_variant    = std::vector<data_variant_t>;

      using filter_type    = std::function<bool(std::size_t, const row_variant&)>;
      using visitor_type   = std::function<void(std::size_t, const row_variant&)>;
      using projector_type = std::function<row_variant(row_variant)>;


      metall_frame(metall_manager_type& mgr, ygm::comm& world, std::string_view key)
      : ygmcomm(world), metallmgr(mgr), df(metall::open_only_t{}, metallmgr.get_local_manager(), key)
      {}

      /// creates a string in persistent memory
      string_t
      persistent_string(std::string_view str) const
      {
        return df.persistent_string(str);
      }

      /// adds a new column with a default value
      /// \tparam ColDescWithDefault: either xpr::dense or xpr::sparse with default value
      /// \param  colname column name
      /// \param  defval a column description and default value
      template <class ColDescWithDefault>
      void add_column_with_default(std::string_view colname, ColDescWithDefault defval)
      {
        df.add_column_with_default(std::move(defval));
        df.name_last_column(colname);
      }


      /// returns the available columns
      ///

      std::vector<std::string>
      get_column_names() const
      {
        return df.get_column_names();
      }



      import_summary
      read_csv_files(
        const std::vector<std::string>&         files,
        std::function<bool(const row_variant&)> filter = accept_all,
        std::function<row_variant(row_variant)> transformer = identity_transformer)
      {
        ygm::io::line_parser lineParser{comm(), files};
        std::size_t          imported      = 0;
        std::size_t          rejected      = 0;
        const std::size_t    initialSize   = df.rows();
        auto                 dataConverter = mk_data_converter(df);

        lineParser.for_all([&imported, &rejected, dfp = &df, filterFn = std::move(filter),
         transFn = std::move(transformer), convFn = std::move(dataConverter)]
                           (const std::string& line) {
                              std::stringstream inp(line);
                              row_variant row = read_tuple_variant(inp, convFn);

                              if (filterFn(row)) {
                                dfp->add(transFn(std::move(row)));
                                ++imported;
                              } else {
                                ++rejected;
                              }
                            });

        assert(df.rows() == initialSize + imported);

        // not necessary here, but common to finish processing all messages
        comm().barrier();

        int totalImported = comm().all_reduce_sum(imported);
        int totalRejected = comm().all_reduce_sum(rejected);

        return {totalImported, totalRejected};
      }

      //
      // filter setters

      /// appends filters and returns *this
      /// \pre A filter \ref fn must not throw
      /// \note *this is returned to allow operation chaining on the container
      ///       e.g., mjl.filter(...).count();
      /// \{
      metall_frame& filter(filter_type fn) {
        filterfn.emplace_back(std::move(fn));
        return *this;
      }

      metall_frame& filter(std::vector<filter_type> fns) {
        std::move(fns.begin(), fns.end(), std::back_inserter(filterfn));
        return *this;
      }
      /// \}

      /// calls \ref accessor with each row, for up to \ref maxrows (per local
      /// container) times
      void for_all_selected(visitor_type accessor, std::size_t maxrows = std::numeric_limits<std::size_t>::max()) const {
        _for_all_selected(std::move(accessor), df, filterfn, maxrows);
      }

      /// returns the number of elements in the local container
      std::size_t local_size() const { return df.rows(); }


      /// returns the number of selected elements in the local container
      std::size_t count_selected() const {
        std::size_t selected = local_size();

        if (filterfn.size()) {
          selected = 0;
          for_all_selected([&selected](std::size_t, const row_variant&) -> void {
            ++selected;
          });
        }

        return selected;
      }

      /// returns the total number of elements
      std::size_t count() const {
        // phase 1: count locally
        std::size_t selected = count_selected();

        // phase 2: reduce globally
        std::size_t totalSelected = ygmcomm.all_reduce_sum(selected);

        return totalSelected;
      }

      /// returns \ref numrows elements from the container
      boost::json::array head(std::size_t numrows, projector_type projector) const {
        using ResultType = decltype(head(numrows, projector));

        ResultType               res;
        std::vector<std::string> remoteRows;
        std::vector<std::size_t> selectedRows;

        msg::mfState =
            msg::process_data_mf{&vector, &remoteRows, &selectedRows, &projector};

        // phase 1: make all local selections
        {
          for_all_selected(
              [&selectedRows](std::size_t rownum, const row_variant&) -> void {
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
          {
            ygmcomm.async(ygmcomm.rank() + 1, msg::row_request{},
                          (numrows - selectedRows.size()));
          }

          for (std::uint64_t i : selectedRows)
            res.emplace_back(projector(vector.at(i)));

          ygmcomm.barrier();
        }

        // phase 3: append received data to res (only rank 0 receives data)
        for (const std::string& row : remoteRows)
          res.emplace_back(boost::json::parse(row));

        return res;
      }



      /// returns the communicator
      ygm::comm& comm() const { return ygmcomm; }


      static
      void create_new(metall_manager_type& manager, ygm::comm&,
                      std::vector<std::string_view> metallkeys) {
        auto& mgr = manager.get_local_manager();

        for (std::string_view key : metallkeys) {
          lines_type vec(metall::create_only_t{}, mgr, key);

          check_condition(vec.valid(), ERR_CONSTRUCT);
        }
      }

      static
      void create_new( metall_manager_type& manager, ygm::comm& comm, std::string_view key) {
        create_new(manager, comm, std::vector<std::string_view>{ key });
      }

      static void check_state(metall_manager_type& manager, ygm::comm&,
                              std::vector<std::string_view> keys) {
        auto& mgr = manager.get_local_manager();

        for (std::string_view key : keys) {
          lines_type vec(metall::open_only_t{}, mgr, key);

          check_condition(vec.valid(), ERR_OPEN);
        }
      }

      static
      void check_state(metall_manager_type& manager, ygm::comm& comm, std::string_view key) {
        check_state(manager, comm, std::vector<std::string_view>{ key });
      }

      static bool accept_all(const row_variant&) { return true; }

      static row_variant identity_transformer(row_variant val) {
        return val;
      }

    private:

      ygm::comm&                ygmcomm;
      metall_manager_type&      metallmgr;
      xpr::dataframe            df;
      std::vector<filter_type>  filterfn = {};

      bool isMainRank() const { return 0 == ygmcomm.rank(); }
      bool isLastRank() const { return 1 == ygmcomm.size() - ygmcomm.rank(); }

      static constexpr char const* ERR_OPEN =
          "unable to open metall_json_lines object";
      static constexpr char const* ERR_CONSTRUCT =
          "unable to construct metall_json_lines object";

      metall_frame()                               = delete;
      metall_frame(metall_frame&&)                 = delete;
      metall_frame(const metall_frame&)            = delete;
      metall_frame& operator=(metall_frame&&)      = delete;
      metall_frame& operator=(const metall_frame&) = delete;
  };
}
