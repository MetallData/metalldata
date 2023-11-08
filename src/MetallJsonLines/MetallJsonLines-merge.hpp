#pragma once

#include <chrono>
#include <ranges>

#include <boost/functional/hash.hpp>
#include <boost/json.hpp>
// #include <metall/json/parse.hpp>

#include "MetallJsonLines.hpp"

// sorry, for introducing this in a header file, which should really be a C
// file.
namespace bj     = boost::json;
namespace mtljsn = metall::json;
namespace xpr    = experimental;

// switch debug output
static constexpr bool DEBUG_TIME_MERGE       = false;
static constexpr bool DEBUG_TRACE_MERGE      = false;
static constexpr bool DEBUG_MERGE_DATA       = false;

// use boost or alternative hash combine
static constexpr bool USE_BOOST_HASH_COMBINE = true;

template <bool On>
struct merge_data_tracer_t
{
    void trace(std::uint64_t llen, std::uint64_t rlen, std::uint64_t klen)
    {
      lhslen += llen;
      rhslen += rlen;
      keylen += klen;

      if (klen > maxkeylen) maxkeylen = klen;
    }

    void datalength(std::uint64_t d) { datalen = d; }

    std::uint64_t lhslen    = 0;
    std::uint64_t rhslen    = 0;
    std::uint64_t keylen    = 0;
    std::uint64_t maxkeylen = 0;
    long double   datalen   = 0.0;
};

template <>
struct merge_data_tracer_t<false>
{
  void trace(std::uint64_t, std::uint64_t, std::uint64_t) {}
  void datalength(std::uint64_t) {}
};

struct simple_logger
{
    simple_logger()
    : os(clippy::clippyLogFile, std::ofstream::app)
    {}

    simple_logger(simple_logger&&) = default;
    ~simple_logger() = default;

    std::ofstream os;

  private:
    simple_logger(const simple_logger&)            = delete;
    simple_logger& operator=(const simple_logger&) = delete;
    simple_logger& operator=(simple_logger&&)      = delete;
};

template <class T>
simple_logger operator<<(simple_logger sl, T obj)
{
  sl.os << obj;

  return std::move(sl);
}

std::ostream& operator<<(std::ostream& os, merge_data_tracer_t<true> el)
{
  return os << "avg(lhslen): " << (el.lhslen / el.datalen)
            << "  avg(rhslen): " << (el.rhslen / el.datalen)
            << "  avg(keylen): " << (el.keylen / el.datalen)
            << "  max(keylen): " << (el.maxkeylen)
            << "  len = " << el.datalen;
}

std::ostream& operator<<(std::ostream& os, merge_data_tracer_t<false>)
{
  return os;
}

using merge_data_tracer = merge_data_tracer_t<DEBUG_MERGE_DATA>;

namespace {

ColumnSelector
append_suffix(const ColumnSelector& list, std::string_view suffix)
{
  ColumnSelector res;

  std::transform( list.begin(), list.end(),
                  std::back_inserter(res),
                  [suffix](std::string_view name) -> std::string
                  {
                    std::string str;

                    str.reserve(name.size() + suffix.size());
                    str.append(name);
                    str.append(suffix);
                    return str;
                  }
                );

  return res;
}

bj::value& valueOf(bj::object& object, const std::string& key) {
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
// alternative hash_combine: https://stackoverflow.com/a/50978188

inline
std::uint64_t xor_shift(std::uint64_t n, int i) { return n ^ (n >> i); }

// a hash function with another name as to not confuse with std::hash
inline
std::uint64_t stable_hash_distribute(std::uint64_t n) {
  std::uint64_t p = 0x5555555555555555ull;    // pattern of alternating 0 and 1
  std::uint64_t c = 17316035218449499591ull;  // random uneven integer constant;
  return c * xor_shift(p * xor_shift(n, 32), 32);
}

inline
std::uint64_t stable_hash_combine(std::uint64_t seed, std::uint64_t comp) {
  const std::uint64_t distr = stable_hash_distribute(comp);

  return std::rotl(seed, std::numeric_limits<std::uint64_t>::digits/3) ^ distr;
}

std::uint64_t combine_hash(std::uint64_t lhs, std::uint64_t rhs) {
  if (!USE_BOOST_HASH_COMBINE)
    return stable_hash_combine(lhs, rhs);

  boost::hash_combine(lhs, rhs);
  return lhs;
}


template <typename MetallJsonAccessor>
std::int64_t json_hash_code(const MetallJsonAccessor& val) {
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

    std::int64_t res{0};

    for (const auto& el : obj) {
      res = combine_hash(res, std::hash<std::string_view>{}(el.key()));
      res = combine_hash(res, json_hash_code(el.value()));
    }

    return res;
  }

  assert(val.is_array());
  std::int64_t res{0};

  // \todo should an element's position be taken into account for the computed
  // hash value?
  for (const auto& el : val.as_array())
    res = combine_hash(res, json_hash_code(el));

  return res;
}

/// define data held locally

enum join_side { lhsData = 0, rhsData = 1 };

struct join_registry : std::tuple<std::uint64_t, int, int> {
  using base = std::tuple<std::uint64_t, int, int>;
  using base::base;

  std::uint64_t hash() const { return std::get<0>(*this); }
  int           owner_rank() const { return std::get<1>(*this); }
  int           owner_index() const { return std::get<2>(*this); }
};

struct by_hash_owner {
  bool operator()(const join_registry& lhs, const join_registry& rhs) const {
    {
      const std::uint64_t lskey = lhs.hash();
      const std::uint64_t rskey = rhs.hash();

      if (lskey < rskey) return true;
      if (lskey > rskey) return false;
    }

    {
      const int lsown = lhs.owner_rank();
      const int rsown = rhs.owner_rank();

      if (lsown < rsown) return true;
      if (lsown > rsown) return false;
    }

    return lhs.owner_index() < rhs.owner_index();
  }
};

struct same_hash_key {
  bool operator()(const join_registry& rhs) const { return h == rhs.hash(); }

  const std::uint64_t h;
};

struct join_info_lhs : std::tuple<int, int> {
  using base = std::tuple<int, int>;
  using base::base;

  int owner() const { return std::get<0>(*this); }
  int index() const { return std::get<1>(*this); }
};

using join_info_rhs = int;

struct merge_candidates
    : std::tuple<std::vector<join_info_rhs>, std::vector<join_info_lhs> > {
  using base =
      std::tuple<std::vector<join_info_rhs>, std::vector<join_info_lhs> >;
  using base::base;

  std::vector<join_info_rhs>&       local_data() { return std::get<0>(*this); }
  const std::vector<join_info_rhs>& local_data() const {
    return std::get<0>(*this);
  }
  std::vector<join_info_lhs>&       remote_data() { return std::get<1>(*this); }
  const std::vector<join_info_lhs>& remote_data() const {
    return std::get<1>(*this);
  }
};

struct join_data : std::tuple<std::vector<int>, bj::array> {
  using base = std::tuple<std::vector<int>, bj::array>;
  using base::base;

  std::vector<int>&       indices() { return std::get<0>(*this); }
  const std::vector<int>& indices() const { return std::get<0>(*this); }
  bj::array&              data() { return std::get<1>(*this); }
  const bj::array&        data() const { return std::get<1>(*this); }
};

using join_index = std::vector<join_registry>;

struct global_process_data {
  std::vector<merge_candidates> mergeCandidates;
  std::vector<join_data>        joinData;
  join_index                    joinIndex[2];
};

global_process_data local;  // global allocation!

///
void store_elem(join_side which, std::uint64_t h, int rank, int idx) {
  local.joinIndex[which].emplace_back(h, rank, idx);

  if (DEBUG_TRACE_MERGE && ((local.joinIndex[which].size() % (1 << 12)) == 0)) {
    simple_logger{}
            << "store_elem: @" << which << " - "
            << local.joinIndex[which].size() << "  from: " << rank << '.'
            << idx << '\n';
  }
}

void comm_join_hash(ygm::comm& w, join_side which, std::uint64_t h, int idx) {
  const int rank = w.rank();
  const int dest = h % w.size();

  if (w.rank() == dest) {
    store_elem(which, h, rank, idx);
    return;
  }

  w.async(
      dest,
      [](join_side operand, std::uint64_t hash, int owner_rank, int owner_idx)
          -> void { store_elem(operand, hash, owner_rank, owner_idx); },
      which, h, rank, idx);
}

template <class PackerFn>
auto pack_join_info(join_index::const_iterator beg, join_index::const_iterator lim,
                    PackerFn fn) -> std::vector<decltype(fn(*beg))> {
  std::vector<decltype(fn(*beg))> res;

  std::transform(beg, lim, std::back_inserter(res), fn);
  return res;
}

std::vector<join_info_lhs> pack_join_info_lhs(join_index::const_iterator beg,
                                              join_index::const_iterator lim) {
  return pack_join_info(beg, lim, [](const join_registry& el) -> join_info_lhs {
    return join_info_lhs{el.owner_rank(), el.owner_index()};
  });
}

std::vector<join_info_rhs> pack_join_info_rhs(join_index::const_iterator beg,
                                              join_index::const_iterator lim) {
  return pack_join_info(beg, lim, [](const join_registry& el) -> join_info_rhs {
    return join_info_rhs{el.owner_index()};
  });
}

void store_candidates(const std::vector<int>&          localInfo,
                     const std::vector<join_info_lhs>& remoteInfo) {
  local.mergeCandidates.emplace_back(localInfo, remoteInfo);
}

void comm_join_candidates(ygm::comm& w, int dest, const std::vector<int>& rhsInfo,
                          const std::vector<join_info_lhs>& lhsInfo) {

  if (DEBUG_TRACE_MERGE)
  {
    simple_logger{}
            << "mc " << dest << rhsInfo.size() << "/" << lhsInfo.size() << '\n';
  }

  if (w.rank() == dest) {
    store_candidates(rhsInfo, lhsInfo);
    return;
  }

  w.async(
      dest,
      [](const std::vector<int>& ri, const std::vector<join_info_lhs>& li)
          -> void { store_candidates(ri, li); },
      rhsInfo, lhsInfo);
}

void store_join_data(const std::vector<int>& indices, const bj::array& data) {
  local.joinData.emplace_back(indices, data);
}

void comm_join_data(ygm::comm& w, int dest, const std::vector<int>& indices,
                  bj::array& data) {
  if (w.rank() == dest) {
    store_join_data(indices, data);
    return;
  }

  std::stringstream buf;

  buf << data;
  w.async(
      dest,
      [](const std::vector<int>& idx, const std::string& data) -> void {
        bj::value jsdata = bj::parse(data);

        assert(jsdata.is_array());

        store_join_data(idx, jsdata.as_array());
      },
      indices, buf.str());
}

// template <typename _allocator_type>
std::uint64_t compute_hash( const xpr::metall_json_lines::accessor_type& val,
                            const ColumnSelector& sel, ygm::comm& w) {
  assert(val.is_object());

  const auto&   obj = val.as_object();
  std::uint64_t res{0};

  for (const ColumnSelector::value_type& col : sel) {
    auto pos = obj.find(col);

    if (pos != obj.end()) {
      const auto& sub = (*pos).value();

      res = combine_hash(res, json_hash_code(sub));
    }
  }

  return res;
}

void compute_merge_info( ygm::comm& world, const xpr::metall_json_lines& vec,
                         const ColumnSelector& colsel, join_side which) {
  vec.for_all_selected(
      [&world, &colsel, which](
          std::size_t                                  rownum,
          const xpr::metall_json_lines::accessor_type& row) -> void {
        std::uint64_t hval = compute_hash(row, colsel, world);

        if (DEBUG_TRACE_MERGE && ((rownum % (1 << 12)) == 0)) {
          simple_logger{}
                  << "@compute_merge_info r:" << world.rank() << ' ' << which
                  << ' ' << rownum << ':' << hval << '\n';
        }

        comm_join_hash(world, which, hval, rownum);
      });

  if (DEBUG_TRACE_MERGE) {
    simple_logger{} << "@compute_merge_info " << which << '\n';
  }
}

void emplace(xpr::metall_json_lines::accessor_type store,
             xpr::metall_json_lines::accessor_type val) {
  if (val.is_string())
    store.emplace_string() = val.as_string().c_str();
  else if (val.is_int64())
    store.emplace_int64() = val.as_int64();
  else if (val.is_uint64())
    store.emplace_uint64() = val.as_uint64();
  else if (val.is_double())
    store.emplace_double() = val.as_double();
  else if (val.is_bool())
    store.emplace_bool() = val.as_bool();

  /*
   *  \todo not yet supported;

      else if (const bj::array* a = val.if_array())
        store.emplace_array(*a);

      else if (const bj::object* o = val.if_object())
        store.emplace_object(*o);
  */
  else {
    assert(val.is_null());
    store.emplace_null();
  }
}

void emplace(xpr::metall_json_lines::accessor_type store,
             const bj::value&                      val) {
  if (const bj::string* s = val.if_string())
    store.emplace_string() = s->c_str();
  else if (const std::int64_t* i = val.if_int64())
    store.emplace_int64() = *i;
  else if (const std::uint64_t* u = val.if_uint64())
    store.emplace_uint64() = *u;
  else if (const double* d = val.if_double())
    store.emplace_double() = *d;
  else if (const bool* b = val.if_bool())
    store.emplace_bool() = *b;

  /*
   *  \todo not yet supported;

      else if (const bj::array* a = val.if_array())
        store.emplace_array(*a);

      else if (const bj::object* o = val.if_object())
        store.emplace_object(*o);
  */
  else {
    assert(val.is_null());
    store.emplace_null();
  }
}

using output_fn = std::function<void(xpr::metall_json_lines::accessor_type::object_accessor, const bj::value&)>;

void
join_records_in_place( xpr::metall_json_lines::accessor_type res,
                       const bj::value& lhs,
                       const output_fn& lhs_append,
                       const bj::value& rhs,
                       const output_fn& rhs_append) {
  auto obj = res.emplace_object();

  lhs_append(obj, lhs);
  rhs_append(obj, rhs);
}

output_fn
make_output_function(ColumnSelector projlst, std::string suffix)
{
  if (projlst.empty())
  {
    // if the projection list is empty, copy over all fields
    return [sf = std::move(suffix)]
           (xpr::metall_json_lines::accessor_type::object_accessor res, const bj::value& val)->void
           {
             assert(val.is_object());
             const auto& that = val.as_object();

             for (const auto& elem : that) {
               const auto& key = elem.key();
               std::string newkey(key.begin(), key.end());

               newkey += sf;
               emplace(res[newkey], elem.value());
             }
           };
  }

  ColumnSelector outFieldList = append_suffix(projlst, suffix);

  // precompute output field list and then copy over selected fields (in projlst)
  return [pl = std::move(projlst), of = std::move(outFieldList)]
         (xpr::metall_json_lines::accessor_type::object_accessor res, const bj::value& val)->void
         {
           assert(val.is_object());

           const auto& that = val.as_object();
           const int   len  = pl.size();

           for (int i = 0; i < len; ++i) {
             if (const bj::value* entry = that.if_contains(pl[i])) {
               emplace(res[of[i]], *entry);
             }
           }
         };
}


bool equal_to(const bj::value&                             lhs,
              const bj::value&                             rhs)
{
  return lhs == rhs;
}

/// \brief Compare JSON Bento value with Boost JSON value.
/// TODO: implement this feature in JSON Bento.
bool equal_to(const xpr::metall_json_lines::accessor_type& lhs,
              const bj::value&                             rhs) {

  if (lhs.is_null()) {
    return rhs.is_null();
  }

  if (lhs.is_bool()) {
    return rhs.is_bool() && (lhs.as_bool() == rhs.as_bool());
  }

  if (lhs.is_int64()) {
    return rhs.is_int64() && (lhs.as_int64() == rhs.as_int64());
  }

  if (lhs.is_uint64()) {
    return rhs.is_uint64() && (lhs.as_uint64() == rhs.as_uint64());
  }

  if (lhs.is_double()) {
    return rhs.is_double() && (lhs.as_double() == rhs.as_double());
  }

  if (lhs.is_string()) {
    const auto  ls = lhs.as_string();
    const auto& rs = rhs.as_string();
    return rhs.is_string() && (ls.size() == rs.size()) &&
           (std::strcmp(ls.data(), rs.data()) == 0);
  }

  if (lhs.is_array()) {
    if (!rhs.is_array()) return false;
    const auto  la = lhs.as_array();
    const auto& ra = rhs.as_array();
    if (la.size() != ra.size()) return false;
    for (std::size_t i = 0; i < la.size(); ++i) {
      if (!equal_to(la[i], ra[i])) return false;
    }
    return true;
  }

  if (lhs.is_object()) {
    if (!rhs.is_object()) return false;
    const auto lo = lhs.as_object();
    const auto& ro = rhs.as_object();
    if (lo.size() != ro.size()) return false;
    for (auto litr : lo) {
      if (!equal_to(litr->value(), ro.at(litr->key().data()))) return false;
    }
    return true;
  }

  assert(false);  // should not reach here
  return false;
}



// keeps a list of keys associated with a hash value
//   and maps a key to an integer.
// \note for a perfect hash the length of the list is small (i.e., 1).
struct key_unifier
{
    using key_type = int;

    /// returns an index for the key identified by obj[keycols..]
    ///   if no such index exists, add the key to the list.
    key_type
    operator()(const bj::value& obj, const ColumnSelector& keycols)
    {
      using iterator = std::vector< internal_key_rep >::const_iterator;

      internal_key_rep thiskey = extract_key(obj, keycols);
      iterator         keysaa  = keys.begin();
      iterator         keyszz  = keys.end();
      auto             keycomp = [thiskeyaa = thiskey.begin()]
                                 (const internal_key_rep& thatkey) -> bool
                                 {
                                   return std::equal( thatkey.begin(), thatkey.end(),
                                                      thiskeyaa,
                                                      [](const bj::value* lhs, const bj::value* rhs)->bool
                                                      {
                                                        return (  (lhs == rhs)
                                                               || (lhs && rhs && (*lhs == *rhs))
                                                               );
                                                      }
                                                    );
                                 };

      if (iterator pos = std::find_if(keysaa, keyszz, keycomp); pos != keyszz)
        return std::distance(keysaa, pos);

      // \todo consider adding a log output if the list exceeds a certain threshold..

      keys.emplace_back(std::move(thiskey));
      assert(keys.size() <= std::numeric_limits<int>::max());
      return keys.size() - 1;
    }

    /// returns an index for the key identified by obj[keycols..]
    ///   if no such index exists, return -1
    template <class JsonObject>
    key_type
    find(const JsonObject& acc, const ColumnSelector& keycols) const
    {
      using iterator       = std::vector< internal_key_rep >::const_iterator;
      using metall_key_rep = decltype(extract_key(acc, keycols));
      using json_element   = metall_key_rep::value_type;

      metall_key_rep   thiskey = extract_key(acc, keycols);
      iterator         keysaa  = keys.begin();
      iterator         keyszz  = keys.end();
      auto             keycomp = [thiskeyaa = thiskey.begin()]
                                 (const internal_key_rep& thatkey) -> bool
                                 {
                                   return std::equal( thatkey.begin(), thatkey.end(),
                                                      thiskeyaa,
                                                      [](const bj::value* lhs, json_element rhs)->bool
                                                      {
                                                        if (!lhs) return !rhs;
                                                        if (!rhs) return false;

                                                        return equal_to(*rhs, *lhs);
                                                      }
                                                    );
                                 };

      if (iterator pos = std::find_if(keysaa, keyszz, keycomp); pos != keyszz)
        return std::distance(keysaa, pos);

      return -1;
    }


    std::size_t len() const { return keys.size(); }

    void clear() { keys.clear(); }

  private:
    using internal_key_rep = std::vector<const bj::value*>;

    template <class JsonValue>
    auto
    extract_key(const JsonValue& val, const ColumnSelector& keycols) const
      -> std::vector<decltype(val.as_object().if_contains(""))>
    {
      using result_type = decltype(extract_key(val, keycols));

      result_type res;
      const auto& obj = val.as_object();

      for (const std::string& key : keycols)
        res.push_back(obj.if_contains(key));

      return res;
    }

    std::vector< internal_key_rep > keys;
};


void add_join_columns_to_output(const ColumnSelector& joincol,
                                ColumnSelector&       output) {
  // if the output is empty, all columns are copied to output anyway.
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

template <class Vector>
void clear_vector(Vector& vec)
{
  Vector v;
  v.swap(vec);
}
}  // namespace

namespace experimental {

std::size_t merge(metall_json_lines& resVec, const metall_json_lines& lhsVec,
                  const metall_json_lines& rhsVec, ColumnSelector lhsOn,
                  ColumnSelector rhsOn, ColumnSelector lhsProj,
                  ColumnSelector rhsProj,
                  std::string lhsSuffix = "_l",
                  std::string rhsSuffix = "_r"
                  ) {
  using time_point = std::chrono::time_point<std::chrono::system_clock>;

  ygm::comm&     world       = resVec.comm();
  ColumnSelector sendListRhs = rhsProj;

  add_join_columns_to_output(rhsOn, sendListRhs);

  //
  // phase 0: build index on corresponding nodes for merge operations
  if (DEBUG_TRACE_MERGE) {
    simple_logger{}
            << "phase 0: @" << world.rank()
            << " *l: " << lhsVec.local_size()  // << " @" << lhsLoc
            << " *r: " << rhsVec.local_size()  // << " @" << rhsLoc
            << '\n';
  }

  time_point starttime_P0 = std::chrono::system_clock::now();

  //   left:
  //     open left object
  //     compute hash and send to designated node
  compute_merge_info(world, lhsVec, lhsOn, lhsData);

  if (DEBUG_TRACE_MERGE) {
    simple_logger{} << "@done left now right\n";
  }

  //   right:
  //     open right object
  //     compute hash and send to designated node
  compute_merge_info(world, rhsVec, rhsOn, rhsData);

  if (DEBUG_TIME_MERGE) {
    time_point endtime_P0 = std::chrono::system_clock::now();
    int elapsedtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endtime_P0 - starttime_P0)
                          .count();

    const double recPerS = ((lhsVec.local_size() + rhsVec.local_size()) /
                             (elapsedtime / 1000.0));

    simple_logger{}
            << "@barrier 0: elapsedTime: " << elapsedtime << "ms : "
            << recPerS << " rec/s\n";
  }

  world.barrier();

  if (DEBUG_TRACE_MERGE) {
    simple_logger{}
            << "phase 1: @" << world.rank()
            << "  L: " << local.joinIndex[lhsData].size()
            << "  R: " << local.joinIndex[rhsData].size()
            << '\n';
  }

  time_point starttime_P1 = std::chrono::system_clock::now();

  // phase 1: perform preliminary merge based on hash
  //       a) sort the two indices
  std::sort(local.joinIndex[lhsData].begin(), local.joinIndex[lhsData].end(),
            by_hash_owner{});
  std::sort(local.joinIndex[rhsData].begin(), local.joinIndex[rhsData].end(),
            by_hash_owner{});

  //       b) send information of join candidates on left side to owners of
  //       right side
  join_index::const_iterator       lsbeg = local.joinIndex[lhsData].begin();
  const join_index::const_iterator lslim = local.joinIndex[lhsData].end();
  join_index::const_iterator       rsbeg = local.joinIndex[rhsData].begin();
  const join_index::const_iterator rslim = local.joinIndex[rhsData].end();

  while ((lsbeg != lslim) && (rsbeg != rslim)) {
    const std::uint64_t       lskey = lsbeg->hash();
    const std::uint64_t       rskey = rsbeg->hash();
    join_index::const_iterator lseqr =
        std::find_if_not(lsbeg + 1, lslim, same_hash_key{lsbeg->hash()});
    join_index::const_iterator rseqr =
        std::find_if_not(rsbeg + 1, rslim, same_hash_key{rsbeg->hash()});

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
    std::vector<join_info_lhs> lhsJoinData = pack_join_info_lhs(lsbeg, lseqr);

    lsbeg = lseqr;

    //     b.2) send lhs candidates to all owners of rhs candidates
    //          processing groups by owner
    while (rsbeg < rseqr) {
      const int dest      = rsbeg->owner_rank();
      auto      sameOwner = [dest](const join_registry& rhs) -> bool {
        return dest == rhs.owner_rank();
      };
      join_index::const_iterator rsdst =
          std::find_if_not(rsbeg + 1, rseqr, sameOwner);

      //           pack all right hand side candidates with the same owner
      std::vector<int> rhsJoinData = pack_join_info_rhs(rsbeg, rsdst);

      //           send candidates
      comm_join_candidates(world, dest, rhsJoinData, lhsJoinData);

      rsbeg = rsdst;
    }
  }

  clear_vector(local.joinIndex[lhsData]);
  clear_vector(local.joinIndex[rhsData]);

  if (DEBUG_TIME_MERGE) {
    time_point endtime_P1 = std::chrono::system_clock::now();
    int elapsedtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endtime_P1 - starttime_P1)
                          .count();

    simple_logger{} << "@barrier 1: elapsedTime: " << elapsedtime
                    << "ms\n";
  }

  world.barrier();  // not needed
  time_point starttime_P2 = std::chrono::system_clock::now();

  if (DEBUG_TRACE_MERGE) {
    simple_logger{} << "phase 2: @" << world.rank()
                    << "  M: " << local.mergeCandidates.size() << '\n';
  }

  // phase 2: send data to node that computes the join
  metall_json_lines::metall_projector_type projectRow = projector(sendListRhs);

  for (const merge_candidates& m : local.mergeCandidates) {
    using iterator = std::vector<join_info_lhs>::const_iterator;

    bj::array jsdata;

    // project the entry according to the projection list and send it to the lhs
    for (int idx : m.local_data())
      jsdata.emplace_back(projectRow(rhsVec.at(idx)));

    // send to all potential owners
    iterator beg = m.remote_data().begin();
    iterator lim = m.remote_data().end();

    assert(beg != lim);
    do {
      const int dest = beg->owner();
      iterator  nxt =
          std::find_if(beg, lim, [dest](const join_info_lhs& el) -> bool {
            return el.owner() != dest;
          });

      std::vector<int> indices;

      std::transform(beg, nxt, std::back_inserter(indices),
                     [](const join_info_lhs& el) -> int { return el.index(); });

      comm_join_data(world, dest, indices, jsdata);

      beg = nxt;
    } while (beg != lim);
  }

  clear_vector(local.mergeCandidates);

  if (DEBUG_TIME_MERGE) {
    time_point endtime_P2 = std::chrono::system_clock::now();
    int elapsedtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endtime_P2 - starttime_P2)
                          .count();

    simple_logger{} << "@barrier 2: elapsedTime: " << elapsedtime
                    << "ms\n";
  }

  world.barrier();

  time_point starttime_P3 = std::chrono::system_clock::now();
  resVec.clear();

  if (DEBUG_TRACE_MERGE) {
    simple_logger{} << "phase 3: @" << world.rank() << "  J: "
                    << local.joinData.size()
                    << '\n';
  }

  // phase 3:
  //   process the join data and perform the actual joins
  {
    ColumnSelector  packListLhs  = lhsProj;
    output_fn       lhsOutFn     = make_output_function(std::move(lhsProj), std::move(lhsSuffix));
    output_fn       rhsOutFn     = make_output_function(std::move(rhsProj), std::move(rhsSuffix));
    key_unifier     keyUnifier;
    merge_data_tracer datatrace;

    add_join_columns_to_output(lhsOn, packListLhs);

    std::vector<key_unifier::key_type> unifiedRhsKeyIndices;

    metall_json_lines::metall_projector_type projectRow = projector(packListLhs);

    for (const join_data& el : local.joinData) {
      const std::size_t rhsDataLen = el.data().size();

      keyUnifier.clear();
      unifiedRhsKeyIndices.clear();
      unifiedRhsKeyIndices.reserve(rhsDataLen);

      // preprocess join data
      for (const bj::value& rhsObj : el.data())
        unifiedRhsKeyIndices.push_back(keyUnifier(rhsObj, rhsOn));

      // \todo this seems to be too sloppy and slowing down performance
      //       -> produce a precise prototype object before retrying resreve
      // resVec.reserve(el.data().front(), el.data().size() * el.indices().size());
      for (int lhsIdx : el.indices()) {
        bj::value             lhsObj = projectRow(lhsVec.at(lhsIdx));

        if (key_unifier::key_type lhsKeyIndex = keyUnifier.find(lhsObj, lhsOn); lhsKeyIndex >= 0) {
          for (std::size_t i = 0; i < rhsDataLen; ++i) {
            if (lhsKeyIndex == unifiedRhsKeyIndices[i])
              join_records_in_place(resVec.append_local(), lhsObj, lhsOutFn, el.data()[i], rhsOutFn);
          }
        }
      }

      datatrace.trace(el.indices().size(), rhsDataLen, keyUnifier.len());
    }

    if (DEBUG_MERGE_DATA)
    {
      datatrace.datalength(local.joinData.size());

      simple_logger{} << datatrace << '\n';
    }
  }

  clear_vector(local.joinData);

  if (DEBUG_TIME_MERGE) {
    time_point endtime_P3 = std::chrono::system_clock::now();
    int elapsedtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          endtime_P3 - starttime_P3)
                          .count();
    simple_logger{} << "@barrier 3: elapsedTime: " << elapsedtime << "ms\n";
  }

  world.barrier();

  if (DEBUG_TRACE_MERGE) {
    simple_logger{} << "phase Z: @" << world.rank() << " *o: "
                    << resVec.local_size() << '\n';
  }

  // done
  return world.all_reduce_sum(resVec.local_size());
}

}  // namespace experimental
