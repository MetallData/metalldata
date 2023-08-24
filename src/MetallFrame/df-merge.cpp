// Copyright 2022 Lawrence Livermore National Security, LLC and other MetallData
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

/// \brief Implements joining two MetallFrame data sets.

#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
//~ #include <bit>

#include <boost/functional/hash.hpp>
#include <boost/json.hpp>

#include "clippy/clippy.hpp"
#include "df-common.hpp"

namespace bj  = boost::json;
namespace jl  = json_logic;
namespace xpr = experimental;

namespace {
const bool DEBUG_TRACE = false;

using StringVector = std::vector<std::string>;

const std::string methodName = "merge";
const std::string ARG_OUTPUT = "output";
const std::string ARG_LEFT   = "left";
const std::string ARG_RIGHT  = "right";

const std::string ARG_HOW     = "how";
const std::string DEFAULT_HOW = "inner";

const std::string ARG_ON       = "on";
const std::string ARG_LEFT_ON  = "left_on";
const std::string ARG_RIGHT_ON = "right_on";

const std::string COLUMNS_LEFT  = "left_columns";
const std::string COLUMNS_RIGHT = "right_columns";

const ColumnSelector DEFAULT_COLUMNS = {};

//~ const std::string    ARG_SUFFIXES     = "suffixes";
//~ const StringVector   DEFAULT_SUFFIXES{"_x", "_y"};
}  // namespace

namespace {
bj::value& valueOf(bj::object& object, const std::string& key) {
  //~ std::cerr << "[" << key << "] = " << object[key]
  //~ << std::endl;
  return object[key];
}

template <typename... argts>
bj::value& valueOf(bj::object& object, const std::string& key,
                   const argts&... inner_keys) {
  bj::value& sub = valueOf(object, key);

  assert(sub.is_object());
  return valueOf(sub.as_object(), inner_keys...);
}

template <class T, typename... argts>
T valueAt(bj::object& value, const argts&... keys) try {
  static constexpr bool requires_container = ::clippy::is_container<T>::value;

  return bj::value_to<T>(
      ::clippy::asContainer(valueOf(value, keys...), requires_container));
} catch (...) {
  return T();
}

//
// hash_combine: https://stackoverflow.com/a/50978188
inline std::uint64_t xorShift(std::uint64_t n, int i) { return n ^ (n >> i); }

// a hash function with another name as to not confuse with std::hash
inline std::uint64_t stableHashDistribute(std::uint64_t n) {
  std::uint64_t p = 0x5555555555555555ull;    // pattern of alternating 0 and 1
  std::uint64_t c = 17316035218449499591ull;  // random uneven integer constant;
  return c * xorShift(p * xorShift(n, 32), 32);
}

std::uint64_t stableHashCombine(std::uint64_t seed, std::uint64_t comp) {
  return boost::hash_combine(seed, comp), seed;
  //~ return std::rotl(seed, std::numeric_limits<std::uint64_t>::digits/3) ^
  //stableHashDistribute(comp);
}

#if OBSOLETE_CODE
template <typename _allocator_type>
std::size_t hashCode(const mtljsn::value<_allocator_type>& val) {
  if (val.is_null()) return std::hash<nullptr_t>{}(nullptr);
  if (val.is_bool()) return std::hash<bool>{}(val.as_bool());
  if (val.is_int64()) return std::hash<std::int64_t>{}(val.as_int64());
  if (val.is_uint64()) return std::hash<std::uint64_t>{}(val.as_uint64());
  if (val.is_double()) return std::hash<double>{}(val.as_double());

  if (val.is_string()) {
    const auto& str = val.as_string();

    return std::hash<std::string_view>{}(std::string_view(str));
  }

  if (val.is_object()) {
    const auto& obj = val.as_object();

    std::size_t res{0};

    for (const auto& el : obj) {
      res = stableHashCombine(res, std::hash<std::string_view>{}(el.key()));
      res = stableHashCombine(res, hashCode(el.value()));
    }

    return res;
  }

  assert(val.is_array());

  std::size_t res{0};

  // \todo should an element's position be taken into account for the computed
  // hash value?
  for (const auto& el : val.as_array())
    res = stableHashCombine(res, hashCode(el));

  return res;
}
#endif /* OBSOLETE_CODE */

std::uint64_t hashCode(xpr::ColumnVariant::pointer_variant_t ptr) {
  if (xpr::string_t** s = std::get_if<xpr::string_t*>(&ptr)) {
    const xpr::string_t& str = **s;

    return std::hash<std::string_view>{}(
        std::string_view{&*str.begin(), str.size()});
  }

  if (xpr::int_t** i = std::get_if<xpr::int_t*>(&ptr))
    return std::hash<xpr::int_t>{}(**i);

  if (xpr::uint_t** u = std::get_if<xpr::uint_t*>(&ptr))
    return std::hash<xpr::uint_t>{}(**u);

  if (xpr::real_t** r = std::get_if<xpr::real_t*>(&ptr))
    return std::hash<xpr::real_t>{}(**r);

  throw std::runtime_error{"unknown column type"};
}

/// define data held locally

enum JoinSide { lhsData = 0, rhsData = 1 };

struct JoinRegistry : std::tuple<std::uint64_t, int, int> {
  using base = std::tuple<std::uint64_t, int, int>;
  using base::base;

  std::uint64_t hash() const { return std::get<0>(*this); }
  int           owner_rank() const { return std::get<1>(*this); }
  int           owner_index() const { return std::get<2>(*this); }
};

struct ByHashOwner {
  bool operator()(const JoinRegistry& lhs, const JoinRegistry& rhs) const {
    std::uint64_t lskey = lhs.hash();
    std::uint64_t rskey = rhs.hash();

    if (lskey < rskey) return true;
    if (lskey > rskey) return false;

    return lhs.owner_rank() < rhs.owner_rank();
  }
};

struct SameHash {
  bool operator()(const JoinRegistry& rhs) const { return h == rhs.hash(); }

  const std::uint64_t h;
};

struct JoinLeftInfo : std::tuple<int, int> {
  using base = std::tuple<int, int>;
  using base::base;

  int owner() const { return std::get<0>(*this); }
  int index() const { return std::get<1>(*this); }
};

using JoinRightInfo = int;

struct MergeCandidates
    : std::tuple<std::vector<JoinRightInfo>, std::vector<JoinLeftInfo> > {
  using base =
      std::tuple<std::vector<JoinRightInfo>, std::vector<JoinLeftInfo> >;
  using base::base;

  std::vector<JoinRightInfo>&       local_data() { return std::get<0>(*this); }
  const std::vector<JoinRightInfo>& local_data() const {
    return std::get<0>(*this);
  }
  std::vector<JoinLeftInfo>&       remote_data() { return std::get<1>(*this); }
  const std::vector<JoinLeftInfo>& remote_data() const {
    return std::get<1>(*this);
  }
};

struct JoinData : std::tuple<std::vector<int>, std::vector<std::string> > {
  using base = std::tuple<std::vector<int>, std::vector<std::string> >;
  using base::base;

  std::vector<int>&               indices() { return std::get<0>(*this); }
  const std::vector<int>&         indices() const { return std::get<0>(*this); }
  std::vector<std::string>&       data() { return std::get<1>(*this); }
  const std::vector<std::string>& data() const { return std::get<1>(*this); }
};

using JoinIndex = std::vector<JoinRegistry>;

struct ProcessData {
  std::vector<MergeCandidates> mergeCandidates;
  std::vector<JoinData>        joinData;
  JoinIndex                    joinIndex[2];
};

ProcessData local;  // global allocation!

///
void storeElem(JoinSide which, std::uint64_t h, int rank, int idx) {
  local.joinIndex[which].emplace_back(h, rank, idx);

  if (DEBUG_TRACE && ((local.joinIndex[which].size() % (1 << 12)) == 0)) {
    //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

    std::cerr << "storeElem: @" << which << " - "
              << local.joinIndex[which].size() << "  from: " << rank << '.'
              << idx << std::endl;
  }
}

void commJoinHash(ygm::comm& w, JoinSide which, std::uint64_t h, int idx) {
  const int rank = w.rank();
  const int dest = h % w.size();

  if (w.rank() == dest) {
    storeElem(which, h, rank, idx);
    return;
  }

  w.async(
      dest,
      [](JoinSide operand, std::uint64_t hash, int owner_rank, int owner_idx)
          -> void { storeElem(operand, hash, owner_rank, owner_idx); },
      which, h, rank, idx);
}

template <class PackerFn>
auto packInfo(JoinIndex::const_iterator beg, JoinIndex::const_iterator lim,
              PackerFn fn) -> std::vector<decltype(fn(*beg))> {
  std::vector<decltype(fn(*beg))> res;

  std::transform(beg, lim, std::back_inserter(res), fn);
  return res;
}

std::vector<JoinLeftInfo> packLeftInfo(JoinIndex::const_iterator beg,
                                       JoinIndex::const_iterator lim) {
  return packInfo(beg, lim, [](const JoinRegistry& el) -> JoinLeftInfo {
    return JoinLeftInfo{el.owner_rank(), el.owner_index()};
  });
}

std::vector<JoinRightInfo> packRightInfo(JoinIndex::const_iterator beg,
                                         JoinIndex::const_iterator lim) {
  return packInfo(beg, lim, [](const JoinRegistry& el) -> JoinRightInfo {
    return JoinRightInfo{el.owner_index()};
  });
}

void storeCandidates(const std::vector<int>&          localInfo,
                     const std::vector<JoinLeftInfo>& remoteInfo) {
  local.mergeCandidates.emplace_back(localInfo, remoteInfo);
}

void commJoinCandidates(ygm::comm& w, int dest, const std::vector<int>& rhsInfo,
                        const std::vector<JoinLeftInfo>& lhsInfo) {
  if (w.rank() == dest) {
    storeCandidates(rhsInfo, lhsInfo);
    return;
  }

  w.async(
      dest,
      [](const std::vector<int>& ri, const std::vector<JoinLeftInfo>& li)
          -> void { storeCandidates(ri, li); },
      rhsInfo, lhsInfo);
}

void storeJoinData(const std::vector<int>&    indices,
                   std::vector<std::string>&& data) {
  local.joinData.emplace_back(indices, std::move(data));
}

void commJoinData(ygm::comm& w, int dest, const std::vector<int>& indices,
                  std::vector<std::string>&& data) {
  if (w.rank() == dest) {
    storeJoinData(indices, std::move(data));
    return;
  }

  w.async(
      dest,
      [](const std::vector<int>& idx, std::string&& data) -> void {
        storeJoinData(idx, std::move(data));
      },
      indices, data);
}

std::uint64_t computeHash(const std::vector<xpr::ColumnVariant>& colaccess,
                          std::int64_t rownum, ygm::comm& w) {
  std::uint64_t res{0};

  for (const xpr::ColumnVariant& col : colaccess)
    res = stableHashCombine(res, hashCode(col.at_variant(rownum)));

  return res;
}

void computeMergeInfo(ygm::comm& world, const vector_json_type& vec,
                      JsonExpression pred, const ColumnSelector& colsel,
                      JoinSide which) {
  std::vector<xpr::ColumnVariant> colaccess =
      vec.get_column_variants_std(colsel);

  auto fn = [&world, &colaccess, which](int rownum) -> void {
    std::uint64_t hval = computeHash(colaccess, rownum, world);

    if (DEBUG_TRACE && ((rownum % (1 << 12)) == 0)) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "@computeMergeInfo r:" << world.rank() << ' ' << which << ' '
                << rownum << ':' << hval << std::endl;
    }

    commJoinHash(world, which, hval, rownum);
  };

  forAllSelected(fn, world.rank(), vec, std::move(pred));

  if (DEBUG_TRACE) {
    //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

    std::cerr << "@computeMergeInfo " << which << std::endl;
  }
}

template <class JsonObject, class JsonValue>
void appendFields(JsonObject& rec, const JsonValue& other,
                  const std::string& other_suffix) {
  //~ static std::uint64_t CNT = 0;

  assert(other.is_object());

  //~ ++CNT;
  const JsonObject& that = other.as_object();

  for (const auto& x : that) {
    const std::string_view& key = x.key();
    std::string             newkey(key.begin(), key.end());
    int                     len = 0;

    newkey += other_suffix;
    rec[newkey] = x.value();

    /*
          if (x.value().is_string())
          {
            const auto& str = x.value().as_string();

            len = str.size();
          }

          if (len < 24)
          {
            if (DEBUG_TRACE)
            {
              std::cerr << CNT << " [" << newkey << "] = " << x.value()
                        << std::endl;
            }

            rec[newkey] = x.value();
          }
    */
  }
}

template <class JsonObject, class JsonValue>
void appendFields(JsonObject& rec, const JsonValue& other,
                  const ColumnSelector& projlst,
                  const std::string&    other_suffix) {
  if (projlst.empty()) {
    appendFields(rec, other, other_suffix);
    return;
  }

  assert(other.is_object());
  const JsonObject& that = other.as_object();

  for (std::string key : projlst) {
    if (const JsonValue* entry = if_contains(that, key)) {
      key += other_suffix;
      rec[key] = *entry;
    }
  }
}

template <class _allocator_type>
void joinRecords(mtljsn::value<_allocator_type>&       res,
                 const mtljsn::value<_allocator_type>& lhs,
                 const ColumnSelector&                 projlstLHS,
                 const mtljsn::value<_allocator_type>& rhs,
                 const ColumnSelector&                 projlstRHS,
                 const std::string&                    lsuf = "_l",
                 const std::string&                    rsuf = "_r") {
  mtljsn::object<_allocator_type>& obj = res.emplace_object();

  appendFields(obj, lhs, projlstLHS, lsuf);
  appendFields(obj, rhs, projlstRHS, rsuf);
}

// computeJoin(*lhsVec, lhsIdx, lhsOn, projLhs, rhsObj, rhsOn, projRhs, outVec);

void computeJoin(const xpr::DataFrame& lhsFrame, std::uint64_t idx,
                 const ColumnSelector& lhsOn, const ColumnSelector& projlstLeft,
                 const std::vector<std::string>& rhsRow,
                 const ColumnSelector&           rhsOn,
                 const ColumnSelector& projlstRight, xpr::DataFrame& res) {
  static std::uint64_t CNT = 0;

  const int N = lhsOn.size();
  assert(N == int(rhsOn.size()));

  for (int i = 0; i < N; ++i) {
    const ColumnSelector::value_type&     lhsCol = lhsOn[i];
    const ColumnSelector::value_type&     rhsCol = rhsOn[i];
    const mtljsn::value<_allocator_type>* lhsSub = if_contains(lhsObj, lhsCol);
    const mtljsn::value<_allocator_type>* rhsSub = if_contains(rhsObj, rhsCol);

    assert(lhsSub && rhsSub);

    if ((*lhsSub) != (*rhsSub)) return;
  }

  if (DEBUG_TRACE) {
    if (((CNT % (1 << 12)) == 0) || (CNT == 1)) std::cerr << "+out = " << CNT;

    ++CNT;
  }

  res.emplace_back();
  joinRecords(res.back(), lhs, projlstLeft, rhs, projlstRight, "_l", "_r");
}

template <class _allocator_type>
mtljsn::value<_allocator_type> convertJsonTypeTo(
    const bj::value& orig, const mtljsn::value<_allocator_type>& /*model*/) {
  return mtljsn::value_from(orig, _allocator_type{});
}

JsonExpression selectionCriteria(bj::object& obj) {
  return valueAt<JsonExpression>(obj, "__clippy_type__", "state", ST_SELECTED);
}

void addJoinColumnsToOutput(const ColumnSelector& joincol,
                            ColumnSelector&       output) {
  // if the output is empty, all columns are copied to output anyway
  if (output.empty()) return;

  std::for_each(joincol.begin(), joincol.end(),
                [&output](const std::string& col) -> void {
                  using Iterator = ColumnSelector::iterator;

                  Iterator const lim = output.end();

                  if (Iterator const pos = std::find(output.begin(), lim, col);
                      pos == lim)
                    output.push_back(col);
                });
}

/*
  void testOutput(bj::object& outObj)
  {
    const bj::string&           outLoc = valueAt<bj::string>(outObj,
  "__clippy_type__", "state", ST_METALL_LOCATION); mtlutil::metall_mpi_adaptor
  outMgr(metall::open_only, outLoc.c_str(), MPI_COMM_WORLD); vector_json_type&
  outVec = jsonVector(outMgr);

    outVec.clear();

    outVec.emplace_back();

    auto& val = outVec.back();
    auto& obj = val.emplace_object();

    obj["test"] = 1;

    std::cerr << "out success " << outLoc << std::endl;
  }
*/
}  // namespace

int ygm_main(ygm::comm& world, int argc, char** argv) {
  using time_point = std::chrono::time_point<std::chrono::system_clock>;

  int            error_code = 0;
  clippy::clippy clip{
      methodName, "For all selected rows, set a field to a (computed) value."};

  // model this as free-standing function
  //~ clip.member_of(CLASS_NAME, "A " + CLASS_NAME + " class");

  // required arguments
  clip.add_required<bj::object>(
      ARG_OUTPUT,
      "result MetallFrame object; any existing data will be overwritten");
  clip.add_required<bj::object>(ARG_LEFT, "right hand side MetallFrame object");
  clip.add_required<bj::object>(ARG_RIGHT, "left hand side MetallFrame object");

  // future optional arguments
  // \todo should these be json expressions
  clip.add_optional<ColumnSelector>(ARG_ON,
                                    "list of column names on which to join on "
                                    "(overruled by left_on/right_on)",
                                    DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(
      ARG_LEFT_ON, "list of columns on which to join left MetallFrame",
      DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(
      ARG_RIGHT_ON, "list of columns on which to join right MetallFrame",
      DEFAULT_COLUMNS);

  // columns to join on
  clip.add_optional<ColumnSelector>(
      COLUMNS_LEFT, "projection list of the left input frame", DEFAULT_COLUMNS);
  clip.add_optional<ColumnSelector>(COLUMNS_RIGHT,
                                    "projection list of the right input frame",
                                    DEFAULT_COLUMNS);

  // currently unsupported optional arguments
  // clip.add_optional(ARG_HOW, "join method:
  // {'left'|'right'|'outer'|'inner'|'cross']} default: inner", DEFAULT_HOW);

  if (clip.parse(argc, argv, world)) {
    return 0;
  }

  try {
    // argument processing
    bj::object outObj = clip.get<bj::object>(ARG_OUTPUT);
    bj::object lhsObj = clip.get<bj::object>(ARG_LEFT);
    bj::object rhsObj = clip.get<bj::object>(ARG_RIGHT);

    ColumnSelector argsOn   = clip.get<ColumnSelector>(ARG_ON);
    ColumnSelector argLhsOn = clip.get<ColumnSelector>(ARG_LEFT_ON);
    ColumnSelector argRhsOn = clip.get<ColumnSelector>(ARG_RIGHT_ON);

    ColumnSelector projLhs = clip.get<ColumnSelector>(COLUMNS_LEFT);
    ColumnSelector projRhs = clip.get<ColumnSelector>(COLUMNS_RIGHT);

    // argument error checking
    //   \todo move to validation
    if (argLhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for left frame."};

    if (argRhsOn.empty() && argsOn.empty())
      throw std::runtime_error{"on-columns unspecified for right frame."};

    const ColumnSelector& lhsOn = argLhsOn.empty() ? argsOn : argLhsOn;
    const ColumnSelector& rhsOn = argRhsOn.empty() ? argsOn : argRhsOn;

    if (lhsOn.size() != rhsOn.size())
      throw std::runtime_error{
          "Number of columns of Left_On and Right_on differ"};

    // addJoinColumnsToOutput(lhsOn, projLhs);
    ColumnSelector sendListRhs = projRhs;

    addJoinColumnsToOutput(rhsOn, sendListRhs);

    // phase 1: build index on corresponding nodes for merge operations
    clip.add_required_state<std::string>(ST_METALL_LOCATION,
                                         "Metall storage location");
    clip.add_required_state<std::string>(ST_METALLFRAME_NAME,
                                         "Metallframe2 key");

    std::string lhsLoc = valueAt<std::string>(lhsObj, "__clippy_type__",
                                              "state", ST_METALL_LOCATION);
    std::string lhsKey = valueAt<std::string>(lhsObj, "__clippy_type__",
                                              "state", ST_METALLFRAME_NAME);
    std::unique_ptr<xpr::DataFrame> lhsVec =
        makeDataFrame(false /* existing */, lhsLoc, lhsKey);
    JsonExpression lhsSel = selectionCriteria(lhsObj);

    std::string rhsLoc = valueAt<std::string>(rhsObj, "__clippy_type__",
                                              "state", ST_METALL_LOCATION);
    std::string rhsKey = valueAt<std::string>(rhsObj, "__clippy_type__",
                                              "state", ST_METALLFRAME_NAME);
    std::unique_ptr<xpr::DataFrame> rhsVec =
        makeDataFrame(false /* existing */, rhsLoc, rhsKey);
    JsonExpression rhsSel = selectionCriteria(rhsObj);

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "phase 0: @" << world.rank() << " *l: " << lhsVec->rows()
                << " @" << lhsLoc << " *r: " << rhsVec->rows() << " @" << rhsLoc
                << std::endl;
    }

    time_point starttime_P1 = std::chrono::system_clock::now();

    //   left:
    //     open left object
    //     compute hash and send to designated node
    computeMergeInfo(world, *lhsVec, lhsSel, lhsOn, lhsData);

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "@done left now right" << std::endl;
    }

    //   right:
    //     open right object
    //     compute hash and send to designated node
    computeMergeInfo(world, *rhsVec, rhsSel, rhsOn, rhsData);

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      time_point endtime_P1 = std::chrono::system_clock::now();
      int elapsedtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                            endtime_P1 - starttime_P1)
                            .count();

      std::cerr << "@barrier 0: elapsedTime: " << elapsedtime << "ms : "
                << ((lhsVec->rows() + rhsVec->rows()) / (elapsedtime / 1000.0))
                << " rec/s" << std::endl;
    }

    world.barrier();

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "phase 1: @" << world.rank()
                << "  L: " << local.joinIndex[lhsData].size()
                << "  R: " << local.joinIndex[rhsData].size() << std::endl;
    }

    // phase 2: perform preliminary merge based on hash
    //       a) sort the two indices
    std::sort(local.joinIndex[lhsData].begin(), local.joinIndex[lhsData].end(),
              ByHashOwner{});
    std::sort(local.joinIndex[rhsData].begin(), local.joinIndex[rhsData].end(),
              ByHashOwner{});

    //       b) send information of join candidates on left side to owners of
    //       right side
    JoinIndex::const_iterator       lsbeg = local.joinIndex[lhsData].begin();
    const JoinIndex::const_iterator lslim = local.joinIndex[lhsData].end();
    JoinIndex::const_iterator       rsbeg = local.joinIndex[rhsData].begin();
    const JoinIndex::const_iterator rslim = local.joinIndex[rhsData].end();

    while ((lsbeg != lslim) && (rsbeg != rslim)) {
      const std::uint64_t       lskey = lsbeg->hash();
      const std::uint64_t       rskey = rsbeg->hash();
      JoinIndex::const_iterator lseqr =
          std::find_if_not(lsbeg + 1, lslim, SameHash{lsbeg->hash()});
      JoinIndex::const_iterator rseqr =
          std::find_if_not(rsbeg + 1, rslim, SameHash{rsbeg->hash()});

      if (lskey < rskey) {
        lsbeg = lseqr;
        continue;
      }

      if (lskey > rskey) {
        rsbeg = rseqr;
        continue;
      }

      //     b.1) keys are equal
      //             pack candidates on left side
      std::vector<JoinLeftInfo> lhsJoinData = packLeftInfo(lsbeg, lseqr);

      lsbeg = lseqr;

      //     b.2) send lhs candidates to all owners of rhs candidates
      //          processing groups by owner
      while (rsbeg < rseqr) {
        const int dest      = rsbeg->owner_rank();
        auto      sameOwner = [dest](const JoinRegistry& rhs) -> bool {
          return dest == rhs.owner_rank();
        };
        JoinIndex::const_iterator rsdst =
            std::find_if_not(rsbeg + 1, rseqr, sameOwner);

        //           pack all right hand side candidates with the same owner
        std::vector<int> rhsJoinData = packRightInfo(rsbeg, rsdst);

        //           send candidates
        commJoinCandidates(world, dest, rhsJoinData, lhsJoinData);

        rsbeg = rsdst;
      }
    }

    // free up space
    local.joinIndex[lhsData].clear();
    local.joinIndex[rhsData].clear();

    world.barrier();  // not needed

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "phase 2: @" << world.rank()
                << "  M: " << local.mergeCandidates.size() << std::endl;
    }

    // phase 3: send data to node that computes the join
    for (const MergeCandidates& m : local.mergeCandidates) {
      using iterator = std::vector<JoinLeftInfo>::const_iterator;

      std::vector<std::string> mrgdata;

      // project the entry according to the projection list and send it to the
      // lhs
      for (int idx : m.local_data())
        projectData(*rhsVec, idx, sendListRhs));

      // send to all potential owners
      iterator beg = m.remote_data().begin();
      iterator lim = m.remote_data().end();

      assert(beg != lim);
      do {
        int      dest = beg->owner();
        iterator nxt =
            std::find_if(beg, lim, [dest](const JoinLeftInfo& el) -> bool {
              return el.owner() != dest;
            });

        std::vector<int> indices;

        std::transform(
            beg, nxt, std::back_inserter(indices),
            [](const JoinLeftInfo& el) -> int { return el.index(); });

        commJoinData(world, beg->owner(), indices, mrgdata);

        beg = nxt;
      } while (beg != lim);
    }

    // free up space
    local.mergeCandidates.clear();

    world.barrier();

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "phase 3: @" << world.rank()
                << "  J: " << local.joinData.size() << std::endl;
    }

    std::string outLoc = valueAt<std::string>(outObj, "__clippy_type__",
                                              "state", ST_METALL_LOCATION);
    std::string outKey = valueAt<std::string>(outObj, "__clippy_type__",
                                              "state", ST_METALLFRAME_NAME);
    std::unique_ptr<xpr::DataFrame> outVec =
        makeDataFrame(false /* existing */, outLoc, outKey);

    outVec->clear();

    // phase 4:
    //   process the join data and perform the actual joins
    {
      using metall_json_value = vector_json_type::value_type;

      for (JoinData& el : local.joinData)
        for (int lhsIdx : el.indices())
          for (std::vector& rhsObj : el.data())
            computeJoin(*lhsVec, lhsIdx, lhsOn, projLhs, rhsObj, rhsOn, projRhs,
                        outVec);
    }

    local.joinData.clear();
    world.barrier();

    if (DEBUG_TRACE) {
      //~ std::ofstream logfile{clippy::clippyLogFile, std::ofstream::app};

      std::cerr << "phase Z: @" << world.rank() << " *o: " << outVec.size()
                << std::endl;
    }

    // done

    const int totalMerged = world.all_reduce_sum(outVec.size());

    if (world.rank() == 0) {
      std::stringstream msg;

      msg << "joined " << totalMerged << " records." << std::endl;
      clip.to_return(msg.str());
    }
  } catch (const std::exception& err) {
    error_code = 1;
    if (world.rank() == 0) clip.to_return(err.what());
  }

  return error_code;
}
